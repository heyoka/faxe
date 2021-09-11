%% Date: 11.05.18 - 21:05
%% Ⓒ 2018 heyoka
-module(graph_builder).
-author("Alexander Minichmair").

%% API
-export([new/0, add_node/3, add_node/4, add_edge/5, add_edge/6, to_graph_def/2]).

new() ->
   digraph:new([acyclic, protected]).

add_node(Graph, NodeId, Component) ->
   add_node(Graph, NodeId, Component, []).
add_node(Graph, NodeId, Component, Metadata) ->
   Inports = df_component:inports(Component),
   OutPorts = df_component:outports(Component),
   Label = #{component => Component, component_pid => nil,
      inports => Inports, outports => OutPorts, metadata => Metadata},
   _NewVertex = digraph:add_vertex(Graph, NodeId, Label).

add_edge(Graph, SourceNode, SourcePort, TargetNode, TargetPort) ->
   add_edge(Graph, SourceNode, SourcePort, TargetNode, TargetPort, []).
add_edge(Graph, SourceNode, SourcePort, TargetNode, TargetPort, Metadata) ->
   Label = #{src_port => SourcePort, tgt_port => TargetPort, metadata => Metadata},
   _NewEdge = digraph:add_edge(Graph, SourceNode, TargetNode, Label).


-spec to_graph_def(digraph:graph(), [{binary(), atom(), term()}]) -> map().
to_graph_def(Graph, NodeList) ->
   Components = digraph_utils:components(Graph),
   F = fun(GroupList, {Index, Out}) ->
      FlatSub = [{El, Index} || El <- GroupList],
      {Index+1, Out++FlatSub}
       end,
   {_Idx, Groups} = lists:foldl(F, {1, []}, Components),
   CFun = fun({NName, NType, _}) ->
      #{<<"name">> => NName, <<"type">> => NType, <<"group">> => proplists:get_value(NName, Groups, 1)}
          end,
   NodesOut = lists:map(CFun, NodeList),
   Edges = [
      #{<<"src">> => Source, <<"src_port">> => PortOut, <<"dest">> => Dest, <<"dest_port">> => PortIn}
      || {_E, Source, Dest, #{src_port := PortOut, tgt_port := PortIn}} <-
         [digraph:edge(Graph, E) || E <- digraph:edges(Graph)]
   ],
   Out = #{nodes => NodesOut, edges => Edges},
   Out.