%%%-------------------------------------------------------------------
%%% @author heyoka
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%% @end
%%%-------------------------------------------------------------------
-module(graph_node_registry).

-behaviour(gen_server).

-export([start_link/0, register_graph_nodes/2, get_graph/1, get_graph_table/0, unregister_graph_nodes/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
  code_change/3]).

-define(SERVER, ?MODULE).
-define(NODE_TO_GRAPH_ETS, node_to_graph).
-define(GRAPH_TO_NODES_ETS, graph_to_nodes).

-record(state, {}).

%%%===================================================================
%%% Spawning and gen_server implementation
%%%===================================================================
-spec register_graph_nodes(pid(), list(pid())) -> list(true).
register_graph_nodes(GraphPid, ComponentPids) when is_pid(GraphPid), is_list(ComponentPids) ->
  AllNodes =
  case ets:lookup(?GRAPH_TO_NODES_ETS, GraphPid) of
    [] ->
      %% first register
      ?SERVER ! {monitor_graph, GraphPid},
      ComponentPids;
    [{GraphPid, Nodes}] -> Nodes ++ ComponentPids
  end,
  ets:insert(?GRAPH_TO_NODES_ETS, {GraphPid, AllNodes}),
  [ets:insert(?NODE_TO_GRAPH_ETS, {CPid, GraphPid}) || CPid <- ComponentPids].

-spec unregister_graph_nodes(pid(), list(pid)) -> list(true).
unregister_graph_nodes(GraphPid, ComponentPids) when is_pid(GraphPid), is_list(ComponentPids) ->
  case ets:lookup(?GRAPH_TO_NODES_ETS, GraphPid) of
    [] ->
      ok;
    [{GraphPid, NodePids}] ->
      ets:insert(?GRAPH_TO_NODES_ETS, {GraphPid, NodePids--ComponentPids})
  end,
  [ets:delete(?NODE_TO_GRAPH_ETS, CPid) || CPid <- ComponentPids].

get_graph_table() ->
  % get graph pid
  lager:notice("~p from ~p",[?FUNCTION_NAME, self()]),
  {ok, GraphPid} = get_graph(self()),
  case ets:lookup(graph_ets, GraphPid) of
    [] ->
      TableId = ets:new(graph_ets_table, [set, public,{read_concurrency,true},{write_concurrency,true}]),
      lager:info("table for graph: ~p has id: ~p",[GraphPid, TableId]),
      ets:insert(graph_ets, {GraphPid, TableId}),
      TableId;
    [{GraphPid, Table}] ->
      lager:info("table already setup for graph: ~p : ~p",[GraphPid, Table]),
      Table
  end.

get_graph(ComponentPid) when is_pid(ComponentPid) ->
  case ets:lookup(?NODE_TO_GRAPH_ETS, ComponentPid) of
    [] -> {error, not_found};
    [{ComponentPid, GraphPid}] -> {ok, GraphPid}
  end.

delete_graph_table(GraphPid) ->
  lager:info("delete_graph_table(~p)",[GraphPid]),
  case ets:lookup(graph_ets, GraphPid) of
    [] -> ok;
    [{GraphPid, Table}] ->
      catch ets:delete(Table),
      ets:delete(graph_ets, GraphPid)
  end.

start_link() ->
  gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

init([]) ->
  {ok, #state{}}.

handle_call(_Request, _From, State = #state{}) ->
  {reply, ok, State}.

handle_cast(_Request, State = #state{}) ->
  {noreply, State}.

handle_info({monitor_graph, GraphPid}, State = #state{}) ->
  erlang:monitor(process, GraphPid),
  {noreply, State};
handle_info({'DOWN', _MonitorRef, process, Pid, _Info}, State=#state{}) ->
  lager:info("Graph is down: ~p",[Pid]),
  %% graph is down, delete ets table and from lookup table
  catch delete_graph_table(Pid),
  case ets:lookup(?GRAPH_TO_NODES_ETS, Pid) of
    [{Pid, Nodes}] -> [ets:delete(?NODE_TO_GRAPH_ETS, NPid) || NPid <- Nodes];
    _ -> ok
  end,
  ets:delete(?GRAPH_TO_NODES_ETS, Pid),
  {noreply, State};
handle_info(_Req, State = #state{}) ->
  {noreply, State}.

terminate(_Reason, _State = #state{}) ->
  ok.

code_change(_OldVsn, State = #state{}, _Extra) ->
  {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
