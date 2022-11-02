%%%-------------------------------------------------------------------
%%% @author heyoka
%%% @copyright (C) 2022, <COMPANY>
%%% @doc
%%% @end
%%%-------------------------------------------------------------------
-module(graph_starter).

-behaviour(gen_server).

-include("faxe.hrl").

-export([start_link/0, start_graph/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
  code_change/3]).

-define(SERVER, ?MODULE).
-define(START_WAIT_TIME, 15).
-define(EMPTY_WAIT_TIME, 200).

-record(state, {
  queue :: queue:queue(),
  timer :: reference(),
  start_count = 0
}).

%%%===================================================================
%%% Spawning and gen_server implementation
%%%===================================================================

start_graph(Graph, Mode) ->
  ?SERVER ! {start_graph, Graph, Mode}.

start_link() ->
  gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

init([]) ->
  {ok, #state{queue = queue:new(), timer = check_queue(?START_WAIT_TIME)}}.

handle_call(_Request, _From, State = #state{}) ->
  {reply, ok, State}.

handle_cast(_Request, State = #state{}) ->
  {noreply, State}.

handle_info(check_queue, State = #state{queue = Q, start_count = Count}) ->
  NewState =
  case queue:is_empty(Q) of
    true ->
      TRef = check_queue(?EMPTY_WAIT_TIME),
%%      lager:info("check_queue, queue is empty"),
      State#state{timer = TRef};
    false ->
      NewQ =
      case queue:out(Q) of
        {{value, {start_graph, Task = #task{name = _Name}, StartMode}}, Q1} ->
          do_start(Task, StartMode),
%%          df_graph:start_graph(Graph, StartMode),
          Q1;
        _ ->
          Q
      end,
      State#state{queue = NewQ, timer = check_queue(?START_WAIT_TIME), start_count = Count+1}
  end,
%%  lager:notice("started ~p flows so far",[NewState#state.start_count]),
  {noreply, NewState};
handle_info({start_graph, _Task = #task{}, _StartMode = #task_modes{}} = Req, State = #state{queue = Q}) ->
  NewQ = queue:in(Req, Q),
  {noreply, State#state{queue = NewQ}};
handle_info(_What, State) ->
  {noreply, State}.

terminate(_Reason, _State = #state{}) ->
  ok.

code_change(_OldVsn, State = #state{}, _Extra) ->
  {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
check_queue(Timeout) ->
  erlang:send_after(Timeout, self(), check_queue).

do_start(T = #task{name = Name, definition = GraphDef},
    #task_modes{concurrency = Concurrency, permanent = Perm} = Mode) ->
  case graph_sup:new(Name, GraphDef) of
    {ok, Graph} ->
      try df_graph:start_graph(Graph, Mode) of
        _ ->
          faxe_db:save_task(T#task{pid = Graph, last_start = faxe_time:now_date(), permanent = Perm}),
          Res =
            case Concurrency of
              1 -> {ok, Graph};
              Num when Num > 1 ->
                faxe:start_concurrent(T, Mode),
                {ok, Graph}
            end,
%%               flow_changed({task, Name, start}),
          Res
      catch
        _:_ = E ->
          lager:error("graph_start_error ~p: ~p", [Name, E]),
          {error, {graph_start_error, E}}
      end;
    {error, {already_started, _Pid}} ->
      lager:notice("task already started: ~p", [Name]),
      {error, already_started}
  end.