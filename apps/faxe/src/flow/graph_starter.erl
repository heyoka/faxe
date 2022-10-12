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
-define(START_WAIT_TIME, 30).
-define(EMPTY_WAIT_TIME, 3000).

-record(state, {
  queue :: queue:queue(),
  timer :: reference()
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

handle_info(check_queue, State = #state{queue = Q}) ->
  NewState =
  case queue:is_empty(Q) of
    true ->
      TRef = check_queue(?EMPTY_WAIT_TIME),
%%      lager:info("check_queue, queue is empty"),
      State#state{timer = TRef};
    false ->
      Len = queue:len(Q),
      lager:info("check_queue, queue Len is ~p",[Len]),
      {StartQ, RestQ} = queue:split(min(Len, 4), Q),
%%      lager:notice("StartQ ~p",[StartQ]),
%%      lager:notice("RestQ ~p",[RestQ]),
      Started =
      plists:map(fun({start_graph, Graph, StartMode}) ->
        df_graph:start_graph(Graph, StartMode) end,
        queue:to_list(StartQ), {processes, schedulers}),
%%      lager:info("started: ~p",[Started]),
      TRef1 = check_queue(?START_WAIT_TIME),
      State#state{queue = RestQ, timer = TRef1}
  end,
  {noreply, NewState};
handle_info({start_graph, _Graph, _StartMode = #task_modes{}} = Req, State = #state{queue = Q, timer = Timer}) ->
  NewTimer =
  case queue:is_empty(Q) of
    true ->
      lager:info("~p start_graph with emtpy queue", [?MODULE]),
      catch timer:cancel(Timer),
      check_queue(?START_WAIT_TIME);
    false ->
      lager:info("~p start_graph", [?MODULE]),
      Timer
  end,
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