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

handle_info({s7_connected, Worker},
    State = #state{pool = Pool, waiting_cons = Waiting, opts = #{ip := Ip}, initial_size = _Initial}) ->

  NewState =
  case lists:member(Worker, Waiting) of
    true ->
      State#state{pool = Pool ++ [Worker], waiting_cons = lists:delete(Worker, Waiting)};
    false ->
      State#state{pool = Pool ++ [Worker]}
  end,
  update_pool(NewState),
  case length(NewState#state.pool) > 0 of
    true -> s7pool_manager ! {up, Ip};
    false -> ok
  end,
  {noreply, NewState};
handle_info({s7_disconnected, Worker},
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
  update_pool(NewState),
  {noreply, NewState};
handle_info({demand, Num}, State = #state{pool = Pool, waiting_cons = Waiting, max_size = MAX}) ->
  Size = length(Pool) + length(Waiting),
  case Num of
    0 ->
      remove_all(State),
      lager:notice("stop ~p [~p] because demand is 0",[?MODULE, State#state.ip]),
      {stop, normal, State};
    _ ->
      NState =
      case Size of
        0 -> add_initial(State); %% demand is > 0, but we have no conns in pool yet, start with initial size
        _ when Num == Size -> State;
        _ when Num > Size, Size == MAX -> State;
        _ when Num > Size -> add_worker(State);
        _ when Num < Size -> remove_worker(State)
      end,
      {noreply, NState}
  end;
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
  update_pool(NewState),
  {noreply, NewState}.

terminate(_Reason, _State = #state{pool = P, waiting_cons = Waiting, ip = Ip}) ->
  [stop_worker(Con) || Con <- P ++ Waiting],
  %% ???
  ets:insert(s7_pools, {Ip, []}).

code_change(_OldVsn, State = #state{}, _Extra) ->
  {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
add_worker(State = #state{waiting_cons = Waiting, opts = Opts}) ->
  {ok, Con} = s7worker:start_link(Opts),
  State#state{waiting_cons = Waiting ++ [Con]}.

remove_worker(State = #state{pool = []}) ->
  State;
remove_worker(State = #state{pool = [Oldest|Pool]}) ->
  stop_worker(Oldest),
  update_pool(State#state{pool = Pool}),
  State#state{pool = Pool}.

remove_all(State = #state{pool = []}) ->
  State;
remove_all(State) ->
  S = remove_worker(State),
  remove_all(S).

update_pool(#state{opts = #{ip := Ip}, pool = Pool}) ->
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
      {ok, Con} = s7worker:start_link(Opts), Con
    end, lists:seq(1, Initial)),
  State#state{waiting_cons = Conns}.