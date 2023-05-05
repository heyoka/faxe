%% Date: 27.12.16 - 17:45
%% Ⓒ 2016 heyoka
-module(df_graph).
-author("Alexander Minichmair").

-behaviour(gen_server).

-include("faxe.hrl").

%% API
-export([start_link/2]).


-export([
   add_node/3,
   add_node/4,
   add_edge/5,
   add_edge/6,
   nodes/1,
   vertices/1,
   edges/1,
   start_graph/2,
   stop/1,
   sink_nodes/1,
   source_nodes/1,
   get_stats/1,
   ping/1,
   start_trace/2,
   stop_trace/1,
   start_subgraph/1,
   stop_subgraph/2,
   graph_def/1,
   start_metrics_trace/2,
   stop_metrics_trace/1]).

%% gen_server callbacks
-export([init/1,
   handle_call/3,
   handle_cast/2,
   handle_info/2,
   terminate/2,
   code_change/3]).

-record(subgraph, {
   vertices,
   edges,
   root_outedges,
   union_node,
   union_inedges
}).

-record(state, {
   id                      :: non_neg_integer() | string(),
   running  = false        :: true | false,
   started  = false        :: true | false,
   graph    = nil,
   start_mode = undefined  :: #task_modes{},
   timeout_ref             :: reference(),
   debug_timeout_ref       :: reference(),
   metrics_timeout_ref     :: reference(),
   nodes    = []           :: list(tuple()),
   nodes_ext_conn = []     :: list(tuple()),
   subgraphs = #{}         :: #{RootNodeName :: binary() => #subgraph{}}
}).

%% node metrics collection interval in ms
-define(METRICS_INTERVAL, 5000).
-define(TRACE_TIMEOUT, 120 * 1000).
%% start timeout
-define(START_GRAPH_TIMEOUT, 7000).

%%%===================================================================
%%% API
%%%===================================================================


-spec(start_link(Id :: term(), Params :: term()) ->
   {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link(Id, Params) ->
   gen_server:start_link(?MODULE, [Id, Params], []).


-spec start_graph(pid(), #task_modes{}) -> {ok, started}|{error, term()}.
start_graph(Graph, Modes) ->
   gen_server:call(Graph, {start, Modes}, ?START_GRAPH_TIMEOUT).

stop(Graph) ->
   erlang:process_flag(trap_exit, true),
   Graph ! stop.
%%   gen_server:call(Graph, {stop}).

%% ping a temporary running graph to keep it alive
ping(Graph) ->
   call(Graph, ping).

add_node(Graph, NodeId, Component) ->
   add_node(Graph, NodeId, Component, []).
add_node(Graph, NodeId, Component, Metadata) ->
   gen_server:call(Graph, {add_node, NodeId, Component, Metadata}).

add_edge(Graph, SourceNode, SourcePort, TargetNode, TargetPort) ->
   add_edge(Graph, SourceNode, SourcePort, TargetNode, TargetPort, []).
add_edge(Graph, SourceNode, SourcePort, TargetNode, TargetPort, Metadata) ->
   gen_server:call(Graph, {add_edge, SourceNode, SourcePort, TargetNode, TargetPort, Metadata}).

start_subgraph(FromVertex) ->
   {ok, GraphPid} = graph_node_registry:get_graph(self()),
   gen_server:call(GraphPid, {make_subgraph, FromVertex}).

stop_subgraph(FromVertex, Port) ->
   {ok, GraphPid} = graph_node_registry:get_graph(self()),
%%   gen_server:call(GraphPid, {delete_subgraph, FromVertex, Port}).
   {Time, Res} = timer:tc(gen_server, call, [GraphPid, {delete_subgraph, FromVertex, Port}]),
   lager:info("Time to stop subgraph: ~p",[Time]),
   Res.

nodes(Graph) ->
   call(Graph, nodes).

graph_def(Graph) ->
   call(Graph, graph_def).

vertices(Graph) ->
   call(Graph, vertices).

edges(Graph) ->
   call(Graph, edges).

sink_nodes(Graph) ->
   call(Graph, sink_nodes).

source_nodes(Graph) ->
   call(Graph, source_nodes).

start_trace(Graph, Duration) ->
   Graph ! {start_trace, Duration}.

stop_trace(Graph) ->
   Graph ! stop_trace.

start_metrics_trace(Graph, Duration) ->
   Graph ! {start_metrics_trace, Duration}.

stop_metrics_trace(Graph) ->
   Graph ! stop_metrics_trace.

get_stats(Graph) ->
   call(Graph, stats).

call(Graph, Mode) ->
   gen_server:call(Graph, {Mode}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%% @doc the graph will be fully configured and connected, ready to be started
-spec(init(Args :: term()) ->
   {ok, State :: #state{}} | {ok, State :: #state{}, timeout() | hibernate} |
   {stop, Reason :: term()} | ignore).
init([Id, #{nodes := Nodes, edges := Edges}]=_T) ->
   lager:md([{flow, Id}]),
   %gen_event:notify(dfevent_graph, {new_graph, Id, self()}),
   Graph = digraph:new([acyclic, protected]),
   lists:foreach(fun(E) -> %gen_event:notify(dfevent_graph, {add_node, Id, E}),
                           build_node(Graph, E) end, Nodes),
   lists:foreach(fun(E) -> %gen_event:notify(dfevent_graph, {add_edge, Id, E}),
                           build_edge(Graph, E) end, Edges),
%%   erlang:send_after(0, self(), {start, push}),
   {ok, #state{graph = Graph, id = Id}}
;
init([Id, _Params]) ->
   Graph = digraph:new([acyclic, protected]),
   {ok, #state{graph = Graph, id = Id}}.

handle_call({add_node, NodeId, Component, Metadata}, _From, State=#state{id = _Id}) ->
   %gen_event:notify(dfevent_graph, {add_node, Id, {NodeId, Component}}),
   Inports = df_component:inports(Component),
   OutPorts = df_component:outports(Component),

   Label = #{component => Component, component_pid => nil,
      inports => Inports, outports => OutPorts, metadata => Metadata},

   _NewVertex = digraph:add_vertex(State#state.graph, NodeId, Label),

   {reply, ok, State};
handle_call({add_edge, SourceNode, SourcePort, TargetNode, TargetPort, Metadata}, _From, State=#state{id = _Id}) ->
   %gen_event:notify(dfevent_graph, {add_edge, Id, {SourceNode, SourcePort, TargetNode, TargetPort}}),
   Label = #{src_port => SourcePort, tgt_port => TargetPort, metadata => Metadata},
   _NewEdge = digraph:add_edge(State#state.graph, SourceNode, TargetNode, Label),
   {reply, ok, State};
handle_call({nodes}, _From, State=#state{nodes = Nodes}) ->
   {reply, Nodes, State};
handle_call({graph_def}, _From, State=#state{graph = Graph, nodes = Nodes}) ->
   Out = graph_builder:to_graph_def(Graph, Nodes),
   {reply, Out, State};
handle_call({vertices}, _From, State = #state{graph = G}) ->
   All = digraph:vertices(G),
   Out = [digraph:vertex(G, V) || V <- All],
   {reply, Out, State};
handle_call({sink_nodes}, _From, State = #state{graph = G}) ->
   OutNodes = digraph:sink_vertices(G),
   {reply, OutNodes, State};
handle_call({source_nodes}, _From, State = #state{graph = G}) ->
   InNodes = digraph:source_vertices(G),
   {reply, InNodes, State};
handle_call({edges}, _From, State = #state{graph = G}) ->
   All = digraph:edges(G),
   Out = [digraph:edge(G, E) || E <- All],
   {reply, Out, State};
handle_call({make_subgraph, FromVertex}, _From, State=#state{}) ->
%%   lager:notice("make_subgraph from Vertex: ~p",[FromVertex]),
   {OutPort, NewState} = clone_and_start_subgraph(FromVertex, State),
   {reply, {ok, OutPort}, NewState};
handle_call({delete_subgraph, FromVertex, Port}, _From, State=#state{}) ->
%%   lager:notice("make_subgraph from Vertex: ~p",[FromVertex]),
   NewState = delete_subgraph(FromVertex, Port, State),
%%   {reply, {ok, 2}, State};
   {reply, ok, NewState};
%% start the computation
handle_call({start, Modes}, _From, State=#state{graph = _G}) ->
   NewState = start(Modes, State),
   Nodes = NewState#state.nodes,
   SinkVertices = digraph:sink_vertices(NewState#state.graph),
   MapFun =
   fun(NodeName) ->
      E = lists:keyfind(NodeName, 2, Nodes),
      {NodeName, element(3, E)}
   end,
   SinkNodes = lists:map(MapFun, SinkVertices),
%%   lager:warning("sink-nodes are: ~p~n nodes are: ~p",[SinkNodes, NewState#state.nodes]),
   {reply, ok, NewState};
handle_call({stop}, _From, State) ->
   do_stop(State),
   {stop, normal, State};
%%handle_call({stats}, _From, State=#state{nodes = Nodes}) ->
%%   Res = [{NodeId, gen_server:call(NPid, stats)} || {NodeId, NPid} <- Nodes],
%%   {reply, Res, State};
handle_call({ping}, _From, State = #state{timeout_ref = TRef, start_mode = #task_modes{temp_ttl = TTL}}) ->
   erlang:cancel_timer(TRef),
   NewTimer = erlang:send_after(TTL, self(), timeout),
   {reply, {ok, TTL}, State#state{timeout_ref = NewTimer}}.

handle_cast(_Request, State) ->
   {noreply, State}.



handle_info({start_trace, Duration}, State = #state{nodes = Nodes, id = Id, debug_timeout_ref = TRef}) ->
   catch erlang:cancel_timer(TRef),
   lager_emit_backend:start_trace(Id),
   [Pid ! start_debug || #node{pid = Pid} <- Nodes],
   TRefNew = erlang:send_after(Duration, self(), stop_trace),
   {noreply, State#state{debug_timeout_ref = TRefNew}};
handle_info(stop_trace, State = #state{nodes = Nodes, id = Id}) ->
   lager_emit_backend:stop_trace(Id),
   [Pid ! stop_debug || #node{pid = Pid} <- Nodes],
   {noreply, State};
handle_info({start_metrics_trace, Duration}, State = #state{id = Id, metrics_timeout_ref = TRef}) ->
   catch erlang:cancel_timer(TRef),
   TRefNew = erlang:send_after(Duration, self(), stop_metrics_trace),
   ets:insert(metric_trace_flows, {Id, true}),
   {noreply, State#state{metrics_timeout_ref = TRefNew}};
handle_info(stop_metrics_trace, State = #state{id = Id}) ->
   lager:info("stop_metrics_trace"),
   ets:delete(metric_trace_flows, Id),
   {noreply, State};
handle_info({start, RunMode}, State) ->
   {noreply, start(RunMode, State)};
handle_info(timeout, State) ->
   lager:notice("Time is out for graph: ~p",[self()]),
   %% delete the task here
   ets:delete(temp_tasks, State#state.id),
   do_stop(State),
   {stop, shutdown, State};
handle_info(collect_metrics, State = #state{nodes = Nodes, id = Id}) ->
   [node_metrics:process_metrics(Id, N) || N <- Nodes],
   erlang:send_after(?METRICS_INTERVAL, self(), collect_metrics),
   {noreply, State};
handle_info(stop, State=#state{}) ->
   do_stop(State),
   %gen_event:notify(dfevent_graph, {stop, Id}),
   {stop, normal, State}.


terminate(_Reason, _State) ->
   ok.

-spec(code_change(OldVsn :: term() | {down, term()}, State :: #state{},
    Extra :: term()) ->
   {ok, NewState :: #state{}} | {error, Reason :: term()}).
code_change(_OldVsn, State, _Extra) ->
   {ok, State}.


%%%===================================================================
%%% Internal functions
%%%===================================================================
do_stop(#state{running = Running, nodes = Nodes, id = Id}) ->
   lager_emit_backend:stop_trace(Id),
   lager:info("stop graph when running:~p",[Running]),
   case Running of
      %% stop all components
      true ->
         %% destroy all metrics (NOT)
%%         lists:foreach(fun({NodeId, Comp, _NPid}) -> node_metrics:destroy(Id, NodeId, Comp) end, Nodes),
         lists:foreach(fun(#node{pid = NPid}) -> NPid ! stop end, Nodes);
      false -> ok
   end,
   timer:sleep(1000).

build_node(Graph, {NodeId, Component}) ->
   build_node(Graph, {NodeId, Component, []});
build_node(Graph, {NodeId, Component, Metadata}) ->
   Inports = df_component:inports(Component),
   OutPorts = df_component:outports(Component),
   Label = #{component => Component, component_pid => nil,
      inports => Inports, outports => OutPorts, metadata => Metadata},
   _NewVertex = digraph:add_vertex(Graph, NodeId, Label).
build_edge(Graph, {SourceNode, SourcePort, TargetNode, TargetPort}) ->
   build_edge(Graph, {SourceNode, SourcePort, TargetNode, TargetPort, []});
build_edge(Graph, {SourceNode, SourcePort, TargetNode, TargetPort, Metadata}) ->
   Label = #{src_port => SourcePort, tgt_port => TargetPort, metadata => Metadata},
   _NewEdge = digraph:add_edge(Graph, SourceNode, TargetNode, Label).


%% @doc
%% creates the initial graph processes and starts the computation
%%
-spec start(#task_modes{}, #state{}) -> #state{}.
start(ModeOpts=#task_modes{run_mode = _RunMode, temporary = Temp, temp_ttl = TTL}, State=#state{graph = G}) ->

%%   erlang:send_after(?METRICS_INTERVAL, self(), collect_metrics),
   Nodes0 = digraph:vertices(G),
   %% make nodes and subscriptions, start df_component-processes and start computing async
   NewState = make_start_nodes(Nodes0, ModeOpts, State),

   %% are we starting temporary ? if yes, start a timer
   TimerRef =
   case Temp of
      false -> undefined;
      true -> erlang:send_after(TTL, self(), timeout)
   end,
   NewState#state{running = true, started = true,
      timeout_ref = TimerRef, start_mode = ModeOpts}.

make_start_nodes(NodeIds, #task_modes{run_mode = RunMode} = StartContext,
    State = #state{graph = G, id = Id, nodes = Nodes0}) when is_list(NodeIds) ->

   %% builds : [{NodeId, Component, Pid}]
   Nodes = build_nodes(NodeIds, G, Id),
   %% Inports and Subscriptions, builds : [{NId, {Ins, list(#subscriptions{})}}]
   Subscriptions = build_subscriptions(Nodes, G, RunMode),
   %% register our pid along with all node(component)-pids for graph ets table handling
   register_nodes(Nodes),
   %% start the nodes with subscriptions
   start_async(Nodes, Subscriptions, StartContext, Id),
   State#state{nodes = Nodes0++Nodes}.

-spec build_nodes(list(binary()), digraph:graph(), binary()) -> [{NodeId :: binary(), Component :: atom(), pid()}].
build_nodes(NodeIds, Graph, Id) ->
   Nodes = lists:map(
      fun(NodeId) ->
         {NodeId, Label} = digraph:vertex(Graph, NodeId),
         #{component := Component, inports := Inports, outports := OutPorts, metadata := Metadata}
            = Label,
         {ok, Pid} = df_component:start_link(Component, Id, NodeId, Inports, OutPorts, Metadata),
         #{'_name' := Name} = Metadata,
         #node{pid = Pid, component = Component, id = NodeId, name = Name}
      end, lists:reverse(NodeIds)),
   Nodes.

-spec build_subscriptions(list({binary(), atom(), pid()}), digraph:graph(), pull|push) ->
   [{NodeId :: binary(), {Ins :: list(), list(#subscription{})}}].
build_subscriptions(Nodes, Graph, RunMode) ->
   Subscriptions =
      lists:foldl(
         fun(#node{id = NId}, Acc) ->
            [{NId, build_node_subscriptions(Graph, NId, Nodes, RunMode)}|Acc]
         end, [], Nodes),
   Subscriptions.

%% @doc build subscriptions for one node in the graph
-spec build_node_subscriptions(digraph:graph(), NodeId :: binary(), tuple(), atom()) -> {list(),list()}.
build_node_subscriptions(Graph, Node, Nodes, FlowMode) ->
   OutEdges = digraph:out_edges(Graph, Node),
%%   lager:notice("build subscriptions for node: ~p :: out-edges: ~p",[Node, OutEdges]),
   Subscriptions = lists:foldl(
      fun(E, Acc) ->
         {_E, V1, V2, Label} = digraph:edge(Graph, E),
         #{src_port := SourcePort, tgt_port := TargetPort, metadata := _Metadata} = Label,
         PubPid = get_node_pid(V1, Nodes),
         SubPid = get_node_pid(V2, Nodes),
         S = df_subscription:new(FlowMode, PubPid, SourcePort, SubPid, TargetPort),
         case proplists:get_value(SourcePort, Acc) of
            undefined ->
               [{SourcePort,[S]}|Acc];
            PSubs when is_list((PSubs)) ->
               [{SourcePort,[S|PSubs]}|proplists:delete(SourcePort, Acc)]
         end
      end,
      [],
      OutEdges),
%%   lager:info("Subscriptions for node: ~p::~p",[Node, Subscriptions]),
   InEdges = digraph:in_edges(Graph, Node),
%%   lager:notice("in-edges: ~p for node: ~p",[InEdges, Node]),
   Inports = lists:map(
      fun(E) ->
         {_E, V1, _V2, _Label = #{tgt_port := TargetPort}} = digraph:edge(Graph, E),
         PubPid = get_node_pid(V1, Nodes),
         {TargetPort, PubPid}
      end,
      InEdges),
   {Inports, Subscriptions}.

-spec start_async(list({binary(), atom(), pid()}), list(), push|pull, binary()) -> ok.
start_async(Nodes0, Subscriptions, StartContext = #task_modes{run_mode = RunMode}, Id) ->
   %% state persistence
   AllStates0 = faxe_db:get_flow_states(Id),
   AllStates = [{NodeId, NState} || #node_state{flownode_id = {_Id, NodeId}} = NState <- AllStates0],
   lager:notice("All node-states from flow ~p: ~p",[Id, AllStates]),
   %% start the nodes with subscriptions
   %% do we have mem nodes present ? then sync start them first
   {Mems, Others} = lists:partition(fun(#node{component = Comp}) -> Comp =:= esp_mem end, Nodes0),
   %% mem nodes first
   lists:foreach(
      fun(#node{id = NodeId, component = _Comp, pid = NPid}) ->
         {Inputs, Subs} = proplists:get_value(NodeId, Subscriptions),
         df_subscription:save_subscriptions({Id, NodeId}, Subs),
%%         node_metrics:setup(Id, NodeId, Comp),
         df_component:start_node(NPid, Inputs, StartContext)
      end,
      Mems),
   lists:foreach(
      fun(#node{id = NodeId, component = _Comp, pid = NPid}) ->
         {Inputs, Subs} = proplists:get_value(NodeId, Subscriptions),
         df_subscription:save_subscriptions({Id, NodeId}, Subs),
%%         node_metrics:setup(Id, NodeId, Comp),
%%         df_component:start_async(NPid, Inputs, RunMode)
         PersistedState =
         case StartContext#task_modes.state_persistence of
            true -> proplists:get_value(NodeId, AllStates);
            false -> undefined
         end,
         lager:info("persistent state for  ~p : ~p",[NodeId, PersistedState]),
         df_component:start_async(NPid, Inputs, StartContext, PersistedState)
      end,
      Others),
   %% if in pull mode, initially let all components send requests to their producers
   maybe_initial_pull(RunMode, Nodes0).

clone_and_start_subgraph(FromVertex, State = #state{subgraphs = Subgraphs, graph = G, nodes = Nodes})
      when not is_map_key(FromVertex, Subgraphs) ->
   Subgraph = clone_subgraph(FromVertex, G, Nodes),

   clone_and_start_subgraph(FromVertex, State#state{subgraphs = Subgraphs#{FromVertex => Subgraph}});
clone_and_start_subgraph(FromVertex,
    State = #state{graph = G, start_mode = StartContext, nodes = ExistingNodes, subgraphs = Subgraphs}) ->

   _S = #subgraph{
      vertices = ReachableVertices,
      edges = CurrentEdges,
      root_outedges = FromOutEdges,
      union_node = UnionNode,
      union_inedges = UnionInEdges
   } =
      maps:get(FromVertex, Subgraphs),

   %% in a first attempt, we say the port-number is equivalent to the number of out-going edges from the FromVertex (out_degree)
   NewOutPort = get_available_outport(G, FromVertex),
   PortBinary = integer_to_binary(NewOutPort),
%%   lager:info("NEW PORT will be ~s",[PortBinary]),
   %% copy these
   CopiedVertices = insert_vertices(G, ReachableVertices, PortBinary),
   insert_edges(G, CopiedVertices, CurrentEdges),

   %% handling the single FromVertex (new edges)
   FromEdgesFun =
      fun(Edge) ->
         {_E, FromVertex, V2, Label} = digraph:edge(G, Edge),
         digraph:add_edge(G, FromVertex, proplists:get_value(V2, CopiedVertices), Label#{src_port => NewOutPort})
      end,
   lists:foreach(FromEdgesFun, FromOutEdges),

   %% connect to the union-node if there is one
   case UnionNode of
      undefined -> ok;
      _ ->
         %% foreach edge going in to the union-node, we must add an edge from the last node(s) before the union node to it
         Fun =
            fun(Edge) ->
               {_E, V1, UnionNode, Label} = digraph:edge(G, Edge),
%%               lager:notice("Edge: ~p~n~p",[Ed, proplists:get_value(V1, CopiedVertices)]),
               _Res = digraph:add_edge(G, proplists:get_value(V1, CopiedVertices), UnionNode, Label)
%%               lager:critical("RES: ~p",[Res])
            end,
         lists:foreach(Fun, UnionInEdges)
   end,

   %% build and start the new node processes
   {_OldVNames, NewVertices} = lists:unzip(CopiedVertices),
   %%%
   NodesNew = build_nodes(NewVertices, G, State#state.id),

   %% not quite elegant here (the next two statements)
   AllNodes = NodesNew++ExistingNodes,
   %% rebuild subscriptions for all nodes ?
   Subscriptions = build_subscriptions(AllNodes, G, StartContext#task_modes.run_mode),

   %% tell the root-node (FromVertex) about it's new subscriptions
   {_Inputs, Subs} = proplists:get_value(FromVertex, Subscriptions),
   df_subscription:save_subscriptions({State#state.id, FromVertex}, Subs),

   %% tell the union-node about it's new inputs (rather than subscriptions)

   %% register our pid along with all node(component)-pids for graph ets table handling
   register_nodes(NodesNew),

   %% start the nodes with subscriptions
   start_async(NodesNew, Subscriptions, StartContext, State#state.id),

   {NewOutPort, State#state{nodes = AllNodes}}.


-spec clone_subgraph(binary(), digraph:graph(), list()) -> #subgraph{}.
clone_subgraph(FromVertex, G, Nodes) when is_binary(FromVertex) ->
   SubgraphVertices = digraph_utils:reachable_neighbours([FromVertex], G),
   Subgraph = digraph_utils:subgraph(G, SubgraphVertices, [{type, inherit}, {keep_labels, true}]),
   Sorted = digraph_utils:topsort(Subgraph),
   SubUnionFun =
   fun(NodeName, {UnionNode, SortedList} = Acc) ->
      case get_node_component(NodeName, Nodes) of
         esp_group_union -> {NodeName, lists:delete(NodeName, SortedList)};
         _ -> case UnionNode of
                 undefined -> Acc;
                 _ -> {UnionNode, lists:delete(NodeName, SortedList)}
              end
      end
   end,
   {UnionNode, SubNodes} = lists:foldl(SubUnionFun, {undefined, Sorted}, Sorted),
   UnionInEdges =
   case UnionNode of
      undefined -> [];
      _ -> InEdges = digraph:in_edges(G, UnionNode),
%%         lager:notice("Union In-Edges: ~p", [lists:map(fun(E) -> digraph:edge(Subgraph, E) end, InEdges)]),
         InEdges
   end,
   S = #subgraph{
      vertices = SubNodes,
      edges = digraph:edges(Subgraph)--UnionInEdges,
      root_outedges = digraph:out_edges(G, FromVertex),
      union_node = UnionNode,
      union_inedges = UnionInEdges
   },
   S.

-spec insert_vertices(digraph:graph(), list(binary()), binary()) -> list({binary(), binary()}).
insert_vertices(G, Vertices, PortBinary) ->
   VerticesMapFun =
      fun(V) ->
         {NodeId, Label} = digraph:vertex(G, V),
         NewNodeId = <<NodeId/binary, "_s", PortBinary/binary>>,
         digraph:add_vertex(G, NewNodeId, Label),
         {NodeId, NewNodeId}
      end,
   CopiedVertices = lists:map(VerticesMapFun, Vertices),
   CopiedVertices.

-spec insert_edges(digraph:graph(), list(tuple()), list()) -> ok.
insert_edges(G, CopiedVertices, CurrentEdges) ->
   EdgesFun =
      fun(Edge) ->
         {_E, V1, V2, Label} = digraph:edge(G, Edge),
         digraph:add_edge(G, proplists:get_value(V1, CopiedVertices), proplists:get_value(V2, CopiedVertices), Label)
      end,
   lists:foreach(EdgesFun, CurrentEdges).


-spec delete_subgraph(binary(), non_neg_integer(), #state{}) -> #state{}.
delete_subgraph(FromVertex, _OutPort, State = #state{subgraphs = Subgraphs})
      when not is_map_key(FromVertex, Subgraphs) ->
   State;
delete_subgraph(FromVertex, OutPort, State = #state{graph = G, subgraphs = Subgraphs, nodes = Nodes, start_mode = Modes}) ->
   #subgraph{vertices = Vertices} = maps:get(FromVertex, Subgraphs),
   OutPortBin = integer_to_binary(OutPort),
   %% delete digraph vertices
   VNames = [<<NodeId/binary, "_s", OutPortBin/binary>> || NodeId <- Vertices],
%%   lager:notice("delete vertices: ~p", [VNames]),
%%   lager:info("node_list all: ~p",[Nodes]),
   digraph:del_vertices(G, VNames),
   %% get old node tuples
   OldNodes = [N || {NodeName, _, _NodePid} = N <- Nodes, lists:member(NodeName, VNames)],
%%   lager:notice("nodes to delete are: ~p",[OldNodes]),
   NewNodes = Nodes--OldNodes,
   OldNodePids = get_node_pids(OldNodes),
   %% unregister nodes
%%   lager:notice("unregister node_pids: ~p",[OldNodePids]),
   graph_node_registry:unregister_graph_nodes(self(), OldNodePids),
%%   lager:notice("stop old nodes: ~p",[OldNodePids]),
   [NodePid ! stop || NodePid <- OldNodePids],
%%   lager:notice("new node list: ~p",[NewNodes]),
   %% now for the subscriptions of the root-vertex
   Subscriptions = build_subscriptions(NewNodes, G, Modes#task_modes.run_mode),
   {_Inputs, Subs} = proplists:get_value(FromVertex, Subscriptions),
   %% update subscriptions for the root node
   df_subscription:save_subscriptions({State#state.id, FromVertex}, Subs),
   State#state{nodes = NewNodes}.

get_available_outport(G, VertexName) ->
   OutEdges = [digraph:edge(G, E) || E <- digraph:out_edges(G, VertexName)],
   Outports = [P || {_E, _V1, _V2, #{src_port := P}} <- OutEdges],
%%   lager:info("existing outports: ~p",[Outports]),
   Degree = length(Outports),
%%   lager:info("~p--~p=~p",[lists:seq(1, Degree), Outports, lists:seq(1, Degree)--Outports]),
   next_port(lists:seq(1, Degree)--Outports, Degree).

next_port([P|_], _Degree) -> P;
next_port([], Degree) -> Degree+1.

-spec register_nodes(list(tuple)) -> list(true).
register_nodes(Nodes) when is_list(Nodes) ->
   graph_node_registry:register_graph_nodes(self(), get_node_pids(Nodes)).

maybe_initial_pull(push, _Nodes) -> ok;
maybe_initial_pull(pull, Nodes) -> lists:foreach(fun(#node{pid = NPid}) -> NPid ! pull end, Nodes).

-spec get_node_pid(binary(), list(tuple())) -> pid()|false.
get_node_pid(NodeId, Nodes) when is_list(Nodes) ->
   #node{pid = NodePid} = lists:keyfind(NodeId, #node.id, Nodes),
   NodePid.
-spec get_node_pids(list(tuple())) -> list(pid()).
get_node_pids(Nodes) when is_list(Nodes) ->
   [NPid || #node{pid = NPid} <- Nodes].

get_node_component(NodeId, Nodes) when is_list(Nodes) ->
   #node{component = Component} = lists:keyfind(NodeId, #node.id, Nodes),
   Component.




