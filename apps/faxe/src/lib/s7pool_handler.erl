%%%-------------------------------------------------------------------
%%% @author heyoka
%%% @copyright (C) 2020, <COMPANY>
%%% @doc
%%% @end
%%%-------------------------------------------------------------------
-module(s7pool_handler).

-behaviour(gen_server).

-export([start_link/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
  code_change/3]).


-record(state, {
  initial_size,
  max_size,
  pool = [],
  ip,
  port,
  opts,
  users,
  monitors,
  pool_index,
  waiting_cons = []
}).

-define(INITIAL_SIZE, 2).
-define(MAX_SIZE, 6).

%%%===================================================================
%%% Spawning and gen_server implementation
%%%===================================================================

start_link(Opts = #{}) ->
  gen_server:start_link(?MODULE, Opts, []).

init(#{ip := Ip, port := Port} = Opts) ->
  erlang:process_flag(trap_exit, true),
  Initial = faxe_config:get(s7pool_initial_size, ?INITIAL_SIZE),
  MaxSize = faxe_config:get(s7pool_max_size, ?MAX_SIZE),
  S = #state{ip = Ip, port = Port, opts = Opts, pool_index = 1, initial_size = Initial, max_size = MaxSize},
  %% start with initial-number of  connections
  {ok, add_initial(S)}.

handle_call(_Request, _From, State = #state{}) ->
  {reply, ok, State}.

handle_cast(_Request, State = #state{}) ->
  {noreply, State}.

handle_info({up, Worker},
    State = #state{pool = Pool, waiting_cons = Waiting, opts = #{ip := Ip}, initial_size = Initial}) ->

%%  lager:notice("s7worker is up!"),
  NewState =
  case lists:member(Worker, Waiting) of
    true ->
%%      lager:warning("found waiting_con now up: ~p",[Worker]),
      State#state{pool = Pool ++ [Worker], waiting_cons = lists:delete(Worker, Waiting)};
    false ->
      lager:alert("s7 worker is up, but not waiting for it: ~p",[Worker]),
      State#state{pool = Pool ++ [Worker]}
  end,
  update_ets(NewState),
  case length(NewState#state.pool) == Initial of
    true -> s7pool_manager ! {up, Ip};
    false -> ok
  end,
%%  lager:alert("[~p] Pool: ~p, Waiting: ~p",[?MODULE, NewState#state.pool, NewState#state.waiting_cons]),
  {noreply, NewState};
handle_info({down, Worker},
    State = #state{pool = Pool, waiting_cons = Waiting, opts = #{ip := Ip}, initial_size = Initial}) ->
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
  case length(NewState#state.pool) == Initial-1 of
    true -> s7pool_manager ! {down, Ip};
    false -> ok
  end,
  {noreply, NewState};
handle_info({demand, Num}, State = #state{pool = Pool, waiting_cons = Waiting, max_size = MAX}) ->
%%  lager:notice("[~p] demand is: ~p",[?MODULE, Num]),
  Size = length(Pool) + length(Waiting),
  NewState =
  case Num of
    0 ->
      remove_all(State);
    _ ->
      case Size of
        0 -> add_initial(State); %% demand is > 0, but we have no conns in pool yet, start with initial size
        _ when Num == Size -> State;
        _ when Num > Size, Size == MAX -> State;
        _ when Num > Size -> add_worker(State);
        _ when Num < Size -> remove_worker(State)
      end
  end,
  {noreply, NewState};
handle_info({'DOWN', _, process, _, _}, State = #state{pool = _Pool}) ->
  {noreply, State#state{}};
handle_info({'EXIT', Pid, Why}, State = #state{pool = Pool, waiting_cons = Waiting}) ->
  lager:warning("S7 Worker exited with Reason: ~p",[Why]),
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

terminate(_Reason, _State = #state{pool = P, waiting_cons = Waiting}) ->
  [stop_worker(Con) || Con <- P ++ Waiting].

code_change(_OldVsn, State = #state{}, _Extra) ->
  {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
add_worker(State = #state{waiting_cons = Waiting, opts = Opts}) ->
  {ok, Con} = s7worker:start_link(Opts),
%%  lager:info("[~p] add worker: ~p", [?MODULE, Con]),
  State#state{waiting_cons = Waiting ++ [Con]}.

remove_worker(State = #state{pool = []}) ->
%%  lager:info("remove worker, when none left"),
  State;
remove_worker(State = #state{pool = [Oldest|Pool]}) ->
%%  lager:info("[~p] remove_worker: ~p",[?MODULE, Oldest]),
  stop_worker(Oldest),
  update_ets(State#state{pool = Pool}),
  State#state{pool = Pool}.

remove_all(State = #state{pool = []}) ->
  State;
remove_all(State) ->
  S = remove_worker(State),
  remove_all(S).

update_ets(#state{opts = #{ip := Ip}, pool = Pool}) ->
  case Pool of
    [] -> s7pool_manager ! {down, Ip};
    _ -> ok
  end,
  ets:insert(s7_pools, {Ip, Pool}).

stop_worker(Pid) ->
  case is_pid(Pid) andalso is_process_alive(Pid) of
    true -> unlink(Pid), catch gen_server:stop(Pid);
    false -> ok
  end.

add_initial(State = #state{initial_size = Initial, opts = Opts}) ->
  %% start with initial-number of  connections
  Conns = lists:map(
    fun(_) ->
      timer:sleep(50),
      {ok, Con} = s7worker:start_link(Opts), Con
    end, lists:seq(1, Initial)),
  State#state{waiting_cons = Conns}.