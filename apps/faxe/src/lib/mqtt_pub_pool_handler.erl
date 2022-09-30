%%%-------------------------------------------------------------------
%%% @author heyoka
%%% @copyright (C) 2020, <COMPANY>
%%% @doc
%%% @end
%%%-------------------------------------------------------------------
-module(mqtt_pub_pool_handler).

-behaviour(gen_server).

-export([start_link/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
  code_change/3]).


-record(state, {
  initial_size,
  max_size,
  max_worker_rate,
  min_worker_rate,
  pool = [],
  host,
  port,
  opts,
  users,
  monitors,
  pool_index,
  waiting_cons = []
}).

%% default configs
-define(INITIAL_SIZE, 2).
-define(MAX_SIZE, 6).
-define(MAX_RATE, 100).
-define(MIN_RATE, 5).

%% add/remove worker mechanism
-define(RATE_INTERVAL_SEC, 10).
-define(RATE_INTERVAL, ?RATE_INTERVAL_SEC * 1000).
-define(MAX_WORKER_CHANGE, 2).

%%%===================================================================
%%% Spawning and gen_server implementation
%%%===================================================================

start_link(Opts = #{}) ->
  gen_server:start_link(?MODULE, Opts, []).

init(#{host := Ip, port := Port} = Opts0) ->
  Opts = Opts0#{pool_caller => self()},
  erlang:process_flag(trap_exit, true),
  Initial = faxe_config:get_sub(mqtt_pub_pool, initial_size, ?INITIAL_SIZE),
  MaxSize = faxe_config:get_sub(mqtt_pub_pool, max_size, ?MAX_SIZE),
  MaxWorkerRate = faxe_config:get_sub(mqtt_pub_pool, worker_max_rate, ?MAX_RATE),
  MinWorkerRate = faxe_config:get_sub(mqtt_pub_pool, worker_min_rate, ?MIN_RATE),

  S = #state{
    host = Ip, port = Port, opts = Opts,
    pool_index = 1,
    initial_size = Initial,
    max_size = MaxSize,
    max_worker_rate = MaxWorkerRate,
    min_worker_rate = MinWorkerRate
    },

  start_rate_timeout(),
  %% start with initial-number of connections
  {ok, add_initial(S)}.

handle_call(_Request, _From, State = #state{}) ->
  {reply, ok, State}.

handle_cast(_Request, State = #state{}) ->
  {noreply, State}.

handle_info(check_rate, State = #state{opts = #{host := Key}, max_worker_rate = MaxRate, min_worker_rate = MinRate}) ->
  CurrentRateCnt = mqtt_pub_pool_manager:get_counter(Key),
  CurrentRate = CurrentRateCnt / ?RATE_INTERVAL_SEC,
  WorkerCount = mqtt_pub_pool_manager:connection_count(Key),
%%  WorkerRate = ceil(CurrentRate / WorkerCount),

  %% how many workers should we have right now ?
  TargetWorkerCount = min(ceil(abs(CurrentRate/MaxRate)), State#state.max_size),
  WorkerRes = TargetWorkerCount - WorkerCount,
  NewState =
  case TargetWorkerCount > State#state.max_size orelse TargetWorkerCount < State#state.initial_size of
    true ->
      State;
    false ->
      case WorkerRes >= 0 of
        true ->
          add_workers(min(WorkerRes, ?MAX_WORKER_CHANGE), State);
        false ->
          remove_workers(min(abs(WorkerRes), ?MAX_WORKER_CHANGE), State)
      end
  end,

%%  lager:notice("current rate count (~p workers): ~p RATE/sec: ~p RATE/sec/worker ~p",
%%    [WorkerCount, CurrentRateCnt, CurrentRate, WorkerRate]),
%%  lager:info("we should have ~p workers",[TargetWorkerCount]),
  start_rate_timeout(),
  mqtt_pub_pool_manager:reset_counter(Key),
  {noreply, NewState};
handle_info({mqtt_connected, Worker},
    State = #state{pool = Pool, waiting_cons = Waiting, opts = #{host := Ip}}) ->

  NewState =
  case lists:member(Worker, Waiting) of
    true ->
      State#state{pool = Pool ++ [Worker], waiting_cons = lists:delete(Worker, Waiting)};
    false ->
      State#state{pool = Pool ++ [Worker]}
  end,
  update_ets(NewState),
  case length(NewState#state.pool) > 0 of
    true -> mqtt_pub_pool_manager ! {up, Ip};
    false -> ok
  end,
%%  lager:alert("[~p] Pool: ~p, Waiting: ~p",[?MODULE, NewState#state.pool, NewState#state.waiting_cons]),
  {noreply, NewState};
handle_info({mqtt_disconnected, Worker},
    State = #state{pool = Pool, waiting_cons = Waiting, opts = #{host := Ip}, initial_size = Initial}) ->
  NewWaiting =
  case lists:member(Worker, Waiting) of
    true -> Waiting; %already waiting for it !
    false ->
      case lists:member(Worker, Pool) of
        true ->%% that we expect !
          [Worker|Waiting]; %% go from pool to waiting
        false -> Waiting %% not what we expect
      end
  end,
  NewState = State#state{pool = lists:delete(Worker, Pool), waiting_cons = NewWaiting},
  update_ets(NewState),
  {noreply, NewState};
handle_info({'DOWN', _, process, _, _}, State = #state{pool = _Pool}) ->
  {noreply, State#state{}};
handle_info({'EXIT', Pid, Why}, State = #state{pool = Pool, waiting_cons = Waiting}) ->
  lager:info("mqtt_pub_pool worker exited with Reason: ~p",[Why]),
  NewState =
  case lists:member(Pid, Pool) of
    true ->
      NewPool0 = lists:delete(Pid, Pool),
      NewWaiting = lists:delete(Pid, Waiting),
      add_worker(State#state{pool = NewPool0, waiting_cons = NewWaiting});
    false -> State
  end,
  update_ets(NewState),
  {noreply, NewState}.

terminate(_Reason, State = #state{pool = P, waiting_cons = Waiting}) ->
  remove_all(State).
%%  [stop_worker(Con) || Con <- P ++ Waiting].

code_change(_OldVsn, State = #state{}, _Extra) ->
  {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
add_worker(State = #state{waiting_cons = Waiting, opts = Opts0}) ->
  %% set a client-id
  Id = faxe_util:to_bin(faxe_util:uuid_string()),
  Opts = Opts0#{client_id => <<"mqtt_pool_", Id/binary>>},
  {ok, Con} = mqtt_publisher:start_link(Opts),
  State#state{waiting_cons = Waiting ++ [Con]}.

add_workers(0, State) ->
  State;
add_workers(Num, State) ->
  S = add_worker(State),
  add_workers(Num-1, S).


remove_worker(State = #state{pool = []}) ->
%%  lager:info("remove worker, when none left"),
  State;
remove_worker(State = #state{pool = [Oldest|Pool]}) ->
%%  lager:info("[~p] remove_worker: ~p",[?MODULE, Oldest]),
  stop_worker(Oldest),
  update_ets(State#state{pool = Pool}),
  State#state{pool = Pool}.

remove_workers(0, State) ->
  State;
remove_workers(Num, State) ->
  S = remove_worker(State),
  remove_workers(Num-1, S).

remove_all(State = #state{pool = []}) ->
  State;
remove_all(State) ->
  S = remove_worker(State),
  remove_all(S).

update_ets(#state{opts = #{host := Ip}, pool = Pool}) ->
  case Pool of
    [] -> mqtt_pub_pool_manager ! {down, Ip};
    _ -> ok
  end,
  ets:insert(mqtt_pub_pools, {Ip, Pool}).

stop_worker(Pid) ->
  case is_pid(Pid) andalso is_process_alive(Pid) of
    true -> unlink(Pid), catch gen_server:stop(Pid);
    false -> ok
  end.

add_initial(State = #state{initial_size = Initial}) ->
  add_workers(Initial, State).

start_rate_timeout() ->
  erlang:send_after(?RATE_INTERVAL, self(), check_rate).