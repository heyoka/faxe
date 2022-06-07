%% Date: 11.05.18 - 21:05
%% Ⓒ 2018 heyoka
-module(graph_builder).
-author("Alexander Minichmair").

-include("faxe.hrl").

%% API
-export([new/0, add_node/3, add_node/4, add_edge/5, add_edge/6, to_graph_def/2, node_opts/0]).

-define(LANG_EXCLUDE_COMPONENTS, [
   c_python,
   esp_triggered_timeout,
   crate_writer,
   esp_deadman_multi,
   esp_extract_path,
   esp_http_post_crate,
   esp_derivative,
   esp_jsonsize,
   esp_kafka_consume,
   esp_modbus,
   esp_mqtt_amqp_bridge,
   esp_jsn_select,
   esp_postgre_out,
   esp_statistics,
   esp_stats,
   esp_sts_sh_resp]).

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
   CFun = fun
             (#node{id = NName, component = NType, name = DisplayName}) ->
                #{<<"name">> => NName, <<"display_name">> => DisplayName, <<"type">> => NType,
                   <<"group">> => proplists:get_value(NName, Groups, 1)};
             ({NodName, NodType, #{'_name' := DisName}}) ->
                #{<<"name">> => NodName, <<"display_name">> => DisName, <<"type">> => NodType,
                   <<"group">> => proplists:get_value(NodName, Groups, 1)}
          end,
   NodesOut = lists:map(CFun, NodeList),
   Edges = [
      #{<<"src">> => Source, <<"src_port">> => PortOut, <<"dest">> => Dest, <<"dest_port">> => PortIn}
      || {_E, Source, Dest, #{src_port := PortOut, tgt_port := PortIn}} <-
         [digraph:edge(Graph, E) || E <- digraph:edges(Graph)]
   ],
   Out = #{nodes => NodesOut, edges => Edges},
   Out.

node_opts() ->
   %% get all components
   All = erlang:loaded(),
%%   ComponentModules = lists:map(fun(Module) -> code:module_info(Module) end, All),
   ComponentModules0 =
      lists:filter(
         fun(Module) ->
            code:ensure_loaded(Module),
            Attrs = Module:module_info(attributes),
            case proplists:get_value(behaviour, Attrs) of
               undefined ->
                  case proplists:get_value(behavior, Attrs) of
                     undefined -> false;
                     Behaviors -> Behaviors == [df_component]
                  end;
               Behaviours -> Behaviours == [df_component]
            end
         end, erlang:loaded()),
   ComponentModules = lists:filter(fun(E) -> not lists:member(E, ?LANG_EXCLUDE_COMPONENTS) end, ComponentModules0),
   lager:notice("~p built-in faxe components", [length(ComponentModules)]),
   lists:map(fun(ModuleAtom) -> node_details(ModuleAtom) end, ComponentModules).
%%   lager:notice("~s", [jiffy:encode(Out)]).


node_details(Module) ->
   NodeName = binary:replace(atom_to_binary(Module), <<"esp_">>, <<>>),
   NodeParams0 = #{
      <<"name">> => NodeName,
      <<"description">> => <<>>,
      <<"documentationUrl">> => <<"https://heyoka.github.io/faxe-docs/site/nodes/", NodeName/binary, ".html">>
   },
   case erlang:function_exported(Module, options, 0) of
      true ->
         ModuleOpts = Module:options(),
         Options = eval_opts(ModuleOpts,[]),
         NodeParams0#{<<"parameters">> => Options};
      false ->
         NodeParams0
   end.

eval_opts([], Acc) ->
   Acc;
eval_opts([#{name := PName, type := PType, default := Default}=O|ModuleOpts], Acc) when is_tuple(Default) ->
   NewAcc = Acc ++ [#{<<"name">> => PName, <<"dataType">> => PType,
      <<"description">> => maps:get(desc, O, <<>>), <<"default">> => <<"from config file">>}],
   eval_opts(ModuleOpts, NewAcc);
eval_opts([#{name := PName, type := PType, default := Default}=O|ModuleOpts], Acc) ->
   NewAcc = Acc ++ [#{<<"name">> => PName, <<"dataType">> => PType,
      <<"description">> => maps:get(desc, O, <<>>), <<"default">> => Default}],
   eval_opts(ModuleOpts, NewAcc);
eval_opts([#{name := PName, type := PType}=O|ModuleOpts], Acc) ->
   NewAcc = Acc ++ [#{<<"name">> => PName, <<"dataType">> => PType,
      <<"description">> => maps:get(desc, O, <<>>)}],
   eval_opts(ModuleOpts, NewAcc);
eval_opts([{PName, PType}|ModuleOpts], Acc) ->
   NewAcc = Acc ++ [#{<<"name">> => PName, <<"dataType">> => PType, <<"description">> => <<>>}],
   eval_opts(ModuleOpts, NewAcc);
eval_opts([{PName, PType, DefaultConfig}|ModuleOpts], Acc) when is_tuple(DefaultConfig)->
   NewAcc = Acc ++ [#{<<"name">> => PName, <<"dataType">> => PType,
      <<"description">> => <<>>, <<"default">> => <<"from config file">>}],
   eval_opts(ModuleOpts, NewAcc);
eval_opts([{PName, PType, Default}|ModuleOpts], Acc) ->
   NewAcc = Acc ++ [#{<<"name">> => PName, <<"dataType">> => PType,
      <<"description">> => <<>>, <<"default">> => Default}],
   eval_opts(ModuleOpts, NewAcc).
