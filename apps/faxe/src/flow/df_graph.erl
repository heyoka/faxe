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
   ping/1, export/1,
   start_trace/1,
   stop_trace/1]).

%% gen_server callbacks
-export([init/1,
   handle_call/3,
   handle_cast/2,
   handle_info/2,
   terminate/2,
   code_change/3, get_errors/1]).

-record(state, {
   id                   :: non_neg_integer() | string(),
   running  = false     :: true | false,
   started  = false     :: true | false,
   graph    = nil,
   start_mode = undefined :: #task_modes{},
   timeout_ref          :: reference(),
   debug_timeout_ref    :: reference(),
   nodes    = []        :: list(tuple()),
   is_leader = false    :: true|false
}).

%% node metrics collection interval in ms
-define(METRICS_INTERVAL, 5000).
-define(TRACE_TIMEOUT, 120 * 1000).
%%%===================================================================
%%% API
%%%===================================================================


-spec(start_link(Id :: term(), Params :: term()) ->
   {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link(Id, Params) ->
   lager:notice("start graph"),
   gen_server:start_link(?MODULE, [Id, Params], []).


-spec start_graph(pid(), #task_modes{}) -> {ok, started}|{error, term()}.
start_graph(Graph, Modes) ->
   gen_server:call(Graph, {start, Modes}).

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

nodes(Graph) ->
   call(Graph, nodes).

vertices(Graph) ->
   call(Graph, vertices).

edges(Graph) ->
   call(Graph, edges).

sink_nodes(Graph) ->
   call(Graph, sink_nodes).

source_nodes(Graph) ->
   call(Graph, source_nodes).

start_trace(Graph) ->
   Graph ! start_trace.

stop_trace(Graph) ->
   Graph ! stop_trace.

get_stats(Graph) ->
   call(Graph, stats).

get_errors(Graph) ->
   call(Graph, get_errors).

export(Graph) ->
   call(Graph, export).

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


handle_call({swarm, begin_handoff} = _Request, _From, State) ->
   lager:notice("~p ~p begin_handoff : ~p",[?MODULE, State#state.id, _Request]),
   {reply, {resume, State}, State};
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
%%handle_call({flownodes}, _From, State = #state{graph = G}) ->
%%   Vs = [digraph:vertex(G, V) || V <- digraph:vertices(G)],
%%   F = fun({NId, #{component := Comp}) ->
%%      {NId, Comp},
%%      end,
%%   Out = [{NId, maps:get(component, DataMap)} || {NId, DataMap} <- Vs],
%%   {reply, Out, State};
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
handle_call({edges}, _From, State) ->
   All = digraph:vertices(State#state.graph),
   {reply, All, State};
%% start the computation
handle_call({start, Modes}, _From, State) ->
   NewState = start(Modes, State),
   {reply, ok, NewState};
handle_call({stop}, _From, State) ->
   do_stop(State),
   {stop, normal, State};
handle_call({stats}, _From, State=#state{nodes = Nodes}) ->

   Res = [{NodeId, gen_server:call(NPid, stats)} || {NodeId, NPid} <- Nodes],
   {reply, Res, State};
handle_call({get_errors}, _From, State=#state{nodes = Nodes}) ->
   GetHistory =
      fun(NodeId) ->
         {NodeId, #{
            <<"processing_errors">> =>
            folsom_metrics:get_history_values(<< NodeId/binary, ?METRIC_ERRORS/binary >>, 24)}
         }
      end,
   Res = [GetHistory(NodeId) || {NodeId, _NPid} <- Nodes],
   {reply, {ok, Res}, State};
handle_call({ping}, _From, State = #state{timeout_ref = TRef, start_mode = #task_modes{temp_ttl = TTL}}) ->
   erlang:cancel_timer(TRef),
   NewTimer = erlang:send_after(TTL, self(), timeout),
   {reply, {ok, TTL}, State#state{timeout_ref = NewTimer}};
handle_call({export}, _From, State = #state{}) ->
%%   Nodes = digraph:vertices(Graph),
%%%%   lager:notice("nodes: ~p~n",[Nodes]),
%%   ExportGraph = digraph_utils:subgraph(Graph, Nodes, [{keep_labels, false}]),
%%   GraphDot = digraph_export:convert(ExportGraph, dot, [pretty]),
%%   {ok, F} = file:open(<<GraphId/binary, ".dot">>, [write]),
%%   io:format(F, "~s~n", [GraphDot]),
   {reply, deprecated, State#state{}}.

handle_cast(_Request, State) ->
   {noreply, State}.



handle_info(start_trace, State = #state{nodes = Nodes, id = Id, debug_timeout_ref = TRef}) ->
   catch erlang:cancel_timer(TRef),
   lager_emit_backend:start_trace(Id),
   [Pid ! start_debug || {_, _, Pid} <- Nodes],
   Timeout = faxe_config:get(debug_time, ?TRACE_TIMEOUT),
   TRefNew = erlang:send_after(Timeout, self(), stop_trace),
   {noreply, State#state{debug_timeout_ref = TRefNew}};
handle_info(stop_trace, State = #state{nodes = Nodes, id = Id}) ->
   lager_emit_backend:stop_trace(Id),
   [Pid ! stop_debug || {_, _, Pid} <- Nodes],
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
   lager:warning("stop graph when running:~p",[Running]),
   case Running of
      %% stop all components
      true ->
         %% destroy all metrics (NOT)
%%         lists:foreach(fun({NodeId, Comp, _NPid}) -> node_metrics:destroy(Id, NodeId, Comp) end, Nodes),
         lists:foreach(fun({_NodeId, _Comp, NPid}) -> NPid ! stop end, Nodes);
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



build_subscriptions(Graph, Node, Nodes, FlowMode) ->
   OutEdges = digraph:out_edges(Graph, Node),
   Subscriptions = lists:foldl(
      fun(E, Acc) ->
         {_E, V1, V2, Label} = digraph:edge(Graph, E),
         #{src_port := SourcePort, tgt_port := TargetPort, metadata := _Metadata} = Label,
         {_, _, PubPid} = lists:keyfind(V1, 1, Nodes),
         {_, _, SubPid} = lists:keyfind(V2, 1, Nodes),
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

   InEdges = digraph:in_edges(Graph, Node),
   Inports = lists:map(
      fun(E) ->
         {_E, V1, _V2, _Label = #{tgt_port := TargetPort}} = digraph:edge(Graph, E),
         {_, _, PubPid} = lists:keyfind(V1, 1, Nodes),
         {TargetPort, PubPid}
      end,
      InEdges),
   {Inports, Subscriptions}.

%% @doc
%% creates the graph processes and starts the computation
%%
-spec start(#task_modes{}, #state{}) -> #state{}.
start(ModeOpts=#task_modes{run_mode = RunMode, temporary = Temp, temp_ttl = TTL},
    State=#state{graph = G, id = Id}) ->
   erlang:send_after(?METRICS_INTERVAL, self(), collect_metrics),
   Nodes0 = digraph:vertices(G),

%% build : [{NodeId, Pid}]
   Nodes = lists:map(
      fun(NodeId) ->
         {NodeId, Label} = digraph:vertex(G, NodeId),
%%         lager:notice("vertex: ~p", [{NodeId, Label}]),
         #{component := Component, inports := Inports, outports := OutPorts, metadata := Metadata}
            = Label,
         {ok, Pid} = df_component:start_link(Component, Id, NodeId, Inports, OutPorts, Metadata),
         {NodeId, Component, Pid}
      end, lists:reverse(Nodes0)),
   %% Inports and Subscriptions
   Subscriptions = lists:foldl(fun({NId, _C, _Pid}, Acc) ->
      [{NId, build_subscriptions(G, NId, Nodes, RunMode)}|Acc]
                               end, [], Nodes),

   %% register our pid along with all node(component)-pids for graph ets table handling
   NodePids = [NPid || {_NodeId, _Comp, NPid} <- Nodes],
   graph_node_registry:register_graph(self(), NodePids),

   %% start the nodes with subscriptions
   lists:foreach(
      fun({NodeId, Comp, NPid}) ->
         {Inputs, Subs} = proplists:get_value(NodeId, Subscriptions),
         node_metrics:setup(Id, NodeId, Comp),
         df_component:start_async(NPid, Inputs, Subs, RunMode)
%%         ,
%%         NPid ! start_debug
%%         NodeStart = df_component:start_node(NPid, Inputs, Subs, FlowMode),
%%         lager:debug("NodeStart for ~p gives: ~p",[NodeId, NodeStart] )
      end,
      Nodes),

   %% if in pull mode initially let all components send requests to their producers
   case RunMode of
      push -> ok;
      pull -> lists:foreach(fun({_NodeId, _C, NPid}) -> NPid ! pull end, Nodes)
   end,
   %% are we starting temporary
   TimerRef =
   case Temp of
      false -> undefined;
      true -> erlang:send_after(TTL, self(), timeout)
   end,
   lager:warning("run_mode is :~p",[RunMode]),
   State#state{running = true, started = true, nodes = Nodes,
      timeout_ref = TimerRef, start_mode = ModeOpts}.
