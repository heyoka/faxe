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
  check_queue(?START_WAIT_TIME),
  {ok, #state{queue = queue:new()}}.

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
        {{value, {start_graph, Graph, StartMode}}, Q1} ->
          df_graph:start_graph(Graph, StartMode),
          Q1;
        _ ->
          Q
      end,
      State#state{queue = NewQ, timer = check_queue(?EMPTY_WAIT_TIME), start_count = Count+1}
  end,
%%  lager:notice("started ~p flows so far",[NewState#state.start_count]),
  {noreply, NewState};
handle_info({start_graph, _Graph, _StartMode = #task_modes{}} = Req, State = #state{queue = Q, timer = Timer}) ->
  catch erlang:cancel_timer(Timer),
  NewTimer = check_queue(?START_WAIT_TIME),
  NewQ = queue:in(Req, Q),
  {noreply, State#state{queue = NewQ, timer = NewTimer}}.

terminate(_Reason, _State = #state{}) ->
  ok.

code_change(_OldVsn, State = #state{}, _Extra) ->
  {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
check_queue(Timeout) ->
  erlang:send_after(Timeout, self(), check_queue).