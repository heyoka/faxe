%% Date: 23.02.17 - 20:24
%% Ⓒ 2017 heyoka
-module(faxe_dfs).
-author("Alexander Minichmair").

-include("faxe.hrl").

%% API
-export([start_script/2, do/0, do/1, do/2, compile/1, file/2, data/2, make_lambda_fun/3, get_all_nodes/0]).

%% for now the only user definable component-type is python(3)
-define(USER_COMPONENT, c_python).
-define(USER_NODE_PREFIX, "@").
-define(USER_COMPONENT_MODULE, cb_module).
-define(USER_COMPONENT_CLASS, cb_class).

%% define a list of lambda library modules for use in lambda expressions
-define(LAMBDA_LIBS, [faxe_lambda_lib, mathex]).

do() ->
   do(<<"graph1">>).
do(Name) ->
   start_script("/home/heyoka/workspace/faxe/apps/faxe/src/window.dfs", Name).
do(File, Name) ->
   start_script(File, Name).

start_script(Script, Name) ->
   try eval(Script) of
      GraphDef when is_map(GraphDef) ->
         {ok, Graph} = dataflow:create_graph(Name, GraphDef),
         ok = dataflow:start_graph(Graph, push),
         Graph
   catch
      Err -> {error, Err}
   end.


-spec file(list(), list()|map()) -> {list(), map()}.
file(ScriptFile, Vars) when is_list(ScriptFile), is_map(Vars) ->
   file(ScriptFile, maps:to_list(Vars));
file(ScriptFile, Vars) when is_list(ScriptFile), is_list(Vars) ->
   DfsParams = faxe_config:get(dfs, ""),
   Path = filename:join(proplists:get_value(script_path, DfsParams, ""), ScriptFile),
%%   lager:info("dfs file path is: ~p",[Path]),
   D = dfs:parse_file(Path, ?LAMBDA_LIBS, Vars, macro_fun()),
   maybe_compile(D).

-spec data(list()|binary(), list()|map()) -> {list(), map()}.
data(DfsData, Vars) when is_map(Vars) ->
   data(DfsData, maps:to_list(Vars));
data(DfsData, Vars) when is_list(Vars) ->
   D = dfs:parse(DfsData, ?LAMBDA_LIBS, Vars, macro_fun()),
   maybe_compile(D).

macro_fun() ->
   fun(MacroName) ->
      case faxe:get_task(MacroName) of
         #task{dfs = MacroDfs} -> MacroDfs;
         _ -> throw("macro '" ++ binary_to_list(MacroName) ++ "' could not be found")
      end
   end.

-spec maybe_compile({tuple(), {list(), list()}}) -> {list(), map()}.
maybe_compile({DFSString, ParserResult}) ->
   case ParserResult of
      {{_Where, line, _LN}, {Keyword, MsgList}} = _M ->
         {error, iolist_to_binary(
            [erlang:atom_to_binary(_Where, utf8), <<" on line ">>,
               integer_to_binary(_LN), <<": ">>,
               erlang:atom_to_binary(Keyword, utf8), <<" ">>, list_to_binary(MsgList)])
         };
      {{_Where, line, _LN}, _Message} = _M ->
         {error, iolist_to_binary(
            [erlang:atom_to_binary(_Where, utf8), <<" on line ">>,
               integer_to_binary(_LN), <<": ">>, list_to_binary(_Message)])
         };
      {_Nodes, _Connections} -> {list_to_binary(DFSString), compile(ParserResult)};
      _ -> {error, ParserResult}
   end.

-spec compile({list(), list()}) -> {error, term()} | map().
compile(D) ->
   lager:notice("dfs compile: ~p", [D]),
   try eval(D) of
      GraphDef when is_map(GraphDef) ->
         #{nodes := Nodes, edges := Edges} = GraphDef,
         [lager:notice("GraphNode: ~p (~p)" ,[NodeName, Type]) || {NodeName, Type, _Params} <- Nodes],
         [lager:notice("GraphEdge from: ~p (~p) to : ~p (~p)  " ,
            [OutNode, OutPort, InNode, InPort]) || {OutNode, OutPort, InNode, InPort, _Opts} <- Edges],
         GraphDef;
      Err -> {error, Err}
   catch
      _:Err:Stack ->
         lager:error("error evaluating dfs result: ~p~n~p",[Err, Stack]),
         {error, Err}
   end.


-spec eval({list(), list()}) -> map().
eval({Nodes, Connections}) ->
   Def = dataflow:new_graph(),

   %% add nodes, handle node options and parameters and build connections
   {NewDef, NewConnections} =
      lists:foldl(
         fun({{NodeName, _Id} = N, Params, Options}, {Def0, Conns}) ->
            {Component, NOpts} = component(NodeName),
            %% build additional connections from node-options
            {NewConns, ParamOptions} = node_conn_params(N, Params),
%%            lager:info("~nafter node_conn_params: ~p",[ParamOptions]),
            %% get component options
            CompOptions0 = component_options(Component, NOpts, NodeName),
%%            lager:info("~nafter component_options: ~p",[CompOptions0]),
            %% set default from config
            CompOptions = eval_options(CompOptions0, []),
%%            lager:info("~nafter eval_options: ~p",[CompOptions]),
            %% convert and assimilate options
            {NName, _Id} = N,
            NOptions = convert_options(NName, CompOptions, lists:flatten(Options ++ ParamOptions)),
%%            lager:warning("here after convert_options"),
            NodeOptions = NOptions ++ NOpts,
%%            lager:notice("~n~p wants options : ~p~n has options: ~p~n~n NodeParameters: ~p",
%%               [Component, CompOptions, Options ++ ParamOptions, NodeOptions]),
            %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
            %% check options with the components option definition
            %% any errors raised here, would be caught in the surrounding call
            FinalOpts = dataflow:build_options(Component, NodeOptions, CompOptions++NOpts),

            NodeId = node_id(N),
            lager:info("all options for node ~p: ~p", [N, FinalOpts]),
            {
               dataflow:add_node({NodeId, Component, FinalOpts}, Def0),
                  Conns ++ NewConns
            }
         end,

         {Def, []},
         Nodes
      ),
%%   lager:info("Got some new NodeConnections: ~p ~n Old : ~p",[NewConnections, Connections]),
   %% connect all
   lists:foldl(
      fun
         ({Successor, Predecessor}, Def1) ->
            dataflow:add_edge({node_id(Predecessor), 1, node_id(Successor), 1, []}, Def1);
         ({Successor, Predecessor, Port}, Def2) ->
            dataflow:add_edge({node_id(Predecessor), 1, node_id(Successor), Port, []}, Def2)
      end,
      NewDef,
      Connections ++ NewConnections
   ).

component(<< ?USER_NODE_PREFIX, Callback/binary>>) ->
   Class = string:titlecase(Callback),
   {?USER_COMPONENT,
      [
         {?USER_COMPONENT_MODULE, atom, list_to_atom(binary_to_list(Callback))},
         {?USER_COMPONENT_CLASS, atom, list_to_atom(binary_to_list(Class))}
      ]
   };
component(NodeName) ->
   {node_name(NodeName), []}.

component_options(?USER_COMPONENT, NOpts, _N) ->
   case erlang:function_exported(?USER_COMPONENT, call_options, 2) of
      true ->
         {_, atom, Callb} = lists:keyfind(?USER_COMPONENT_MODULE, 1, NOpts),
         {_, atom, ClassName} = lists:keyfind(?USER_COMPONENT_CLASS, 1, NOpts),
         ?USER_COMPONENT:call_options(Callb, ClassName);
      false -> []
   end;
component_options(Component, _NOpts, NodeName) ->
   case erlang:function_exported(Component, options, 0) of
      true -> Component:options();
      false -> throw("Component '" ++ binary_to_list(NodeName) ++ ":options' not found")
   end.


%% evaluate default config options
eval_options([], Acc) ->
   Acc;
eval_options([{OptName, OptType, CKey}|Opts], Acc) when is_tuple(CKey) ->
   OptVal = conf_val(CKey),
   eval_options(Opts, Acc ++ [{OptName, OptType, OptVal}]);
eval_options([Opt|Opts], Acc) ->
   eval_options(Opts, Acc ++ [Opt]).

conf_val({ConfigKey, ConfigSubKey}) ->
   ConfigData = application:get_env(faxe, ConfigKey, []),
   conv_config_val(proplists:get_value(ConfigSubKey, ConfigData));
conf_val({ConfigKey, ConfigSubKey, ConfigSubSubKey}) ->
   Value =
   case application:get_env(faxe, ConfigKey, []) of
      [] -> [];
      ConfData when is_list(ConfData) ->
         case proplists:get_value(ConfigSubKey, ConfData) of
            undefined -> undefined;
            ConfSubData when is_list(ConfSubData) -> proplists:get_value(ConfigSubSubKey, ConfSubData)
         end
   end,
   conv_config_val(Value).

%% @doc we know config does not give us any binary values, but in faxe we only use
%% binaries for strings, so we convert any string(list) val to binary
%% at the moment this is dangerous, as we do not know exactly whether the list was meant
%% to be a string
conv_config_val([]) -> [];
conv_config_val(Val) when is_list(Val) ->
   list_to_binary(Val);
conv_config_val(Val) -> Val.

node_id({Name, Id}) when is_binary(Name) andalso is_integer(Id) ->
   <<Name/binary, (integer_to_binary(Id))/binary>>.

node_params({NodeName, _Id}=N, NodeParams) ->
   case faxe_node_params:options(NodeName) of
      undefined -> [];
      [{all, Name, _T}] -> node_params_all(NodeParams, Name);
      _Other when is_list(_Other) -> node_params_each(N, NodeParams)
   end.

%% it is assumed, that all NodeParams are of the same type
node_params_all(NodeParams, Name) ->
   Params =
      lists:foldl(
         fun
            ({lambda, FunString, BinRefs, FunParams}, Params0) ->
               Fun = make_lambda_fun(FunString, FunParams, BinRefs),
               [Fun] ++ Params0;
            ({_Ptype, PVal}=_V, Params0) ->
               [PVal] ++ Params0
         end,
         [],
         NodeParams),
   [{Name, {list, Params}}].

node_params_each({NodeName, _Id}=_N, NodeParams) ->
   {Parameters, _Idx} =
      lists:foldl(
         fun
            ({lambda, FunString, BinRefs, FunParams}, {Params0, Index0}) ->
               OptParams = faxe_node_params:options(NodeName),
               NewParams =
                  case catch(lists:nth(Index0, OptParams)) of
                     {Index0, ParamName, _PType} ->
                        Fun = make_lambda_fun(FunString, FunParams, BinRefs),
                        [{ParamName, [Fun]}|Params0];

                     _Error -> throw("Illegal or missing Lambda Parameter " ++
                        integer_to_list(Index0) ++ " for node '" ++ binary_to_list(NodeName)++"'" )
                  end,
               {NewParams, Index0+1};
            ({Ptype, PVal}=_V, {Params0, Index0}) ->
               OptParams = faxe_node_params:options(NodeName),
               NewParams =
                  case catch(lists:nth(Index0, OptParams)) of
                     {Index0, ParamName, _PType} -> [{ParamName, [{Ptype, PVal}]}|Params0];

                     _Error -> throw("Illegal or missing Parameter " ++
                        integer_to_list(Index0) ++ " for node '" ++ binary_to_list(NodeName)++"'" )
                  end,
%%               lager:error("NodeParam: ~p :: ~p", [V, NewParams]),
               {NewParams, Index0+1}
         end,
         {[], 1},
         NodeParams),
   Parameters.

node_conn_params({NodeName, _Id}=N, NodeParams) ->
   {NewConns, Parameters, _Idx} =
      lists:foldl(
         fun
            ({connect, {_NodeName, _CNodeId}=CNode}, {Cs, Params, Index}) ->
               CParams = faxe_node_params:params(NodeName),
               NewConnections =
               case CParams of
                  {all, new_port, StartPort} -> [{N, CNode, StartPort+Index}|Cs];
                  _ -> case catch(lists:nth(Index, CParams)) of
                          {all, NPort} -> [{N, CNode, NPort}|Cs];


                          {Index, NPort} -> [{N, CNode, NPort}|Cs];

                          _Error ->
                             throw("Illegal or missing Connection Parameter " ++
                                integer_to_list(Index) ++ " for node '"
                                ++ binary_to_list(NodeName)++"'" )
                       end
               end,
               {NewConnections, Params, Index+1};
            (Par, {Cs0, Params0, Index0}) -> {Cs0, [Par]++Params0, Index0+1}
         end,
         {[], [], 1},
         NodeParams
      ),
   NewNodeParams = node_params(N, Parameters),
   {NewConns, NewNodeParams}.

-spec convert_options(binary(), list(), list()) -> list({binary(),list()}).
convert_options(NodeName, NodeOptions, Params) ->
   Opts = lists:foldl(
      fun
         ({Name, Type, _Def}, O) -> [{erlang:atom_to_binary(Name, utf8), {Name, Type}}|O];
         ({Name, Type}, O)       -> [{erlang:atom_to_binary(Name, utf8), {Name, Type}}|O];
         %% ignore port connectors
         (_, O)                  -> O
      end,
      [],
      NodeOptions
   ),
   lists:foldl(
      fun
         ({PName, PVals}, Acc) ->
%%            lager:warning("~p :: ~p~n",[PName, proplists:get_value(PName, Opts)]),
         case proplists:get_value(PName, Opts) of
            undefined -> %% unspecified option
               %% check for similar options
               OptNames = [binary_to_list(OName) || {OName, _} <- Opts],
               SimilarOptions = lists:filter(fun({Dist, _}) -> Dist < 4 end,
                  lev_dist(binary_to_list(PName), OptNames)),
               Add = case SimilarOptions of
                        [] -> "";
                        _ -> ", did you mean: " ++ lists:flatten(
                           lists:join(" or ", ["'" ++ O ++ "'" || {_Dist, O} <- SimilarOptions])) ++ " ?"
                     end,
               throw("Unknown option '" ++ binary_to_list(PName)
                  ++"' for node '" ++ binary_to_list(NodeName) ++ "'" ++ Add);
            {Name, param_list = Type} ->
               {value, {Name, Type, POpts}, _L} = lists:keytake(Name, 1, NodeOptions),
%%               lager:warning("~nconvert param_list(~p, ~p, ~p, ~p)",[Name, Type, PVals, POpts]),
               Zipped = lists:zip(POpts, PVals),
               C = [convert(N, T, [PV]) || {{N, T}, PV} <- Zipped],
               [{Name, C} | Acc];
            {_Name, _Type} when PVals == {list, []} ->
%%               lager:info("~nconvert_list(~p, ~p, ~p)",[Name, Type, PVals]),
               Acc;
            {Name, Type} ->
%%               lager:info("~nconvert(~p, ~p, ~p)",[Name, Type, PVals]),
               [convert(Name, Type, PVals) | Acc]

         end
      end,
      [],
      Params

   ).

-spec convert(binary(), atom(), list()) -> tuple().
convert(Name, Type, PVals) ->
%%   lager:notice("convert(~p,~p,~p)",[Name, Type, PVals]),
   TName = erlang:atom_to_binary(Type, utf8),
%%   lager:warning("converted: ~p",[TName]),
   case estr:str_ends_with(TName, <<"list">>) of
      true -> {Name, list_params(Type, PVals)};
      false -> case length(PVals) of
                  0 -> {Name, cparam(Type, [])};
                  1 ->
%%                     lager:warning("pvals is: ~p",[PVals]),
                     {Name, cparam(Type, hd(PVals))};
                  _ ->
                     PValList =
                     case PVals of
                          {list, PVals1} -> PVals1;
                           L when is_list(L) -> L
                       end,
                     {Name, list_params(PValList)}
               end
   end.

list_params(Type, {list, Vals}) ->
%%   lager:notice("~n ~p PVals: ~p", [Type, Vals]),
   list_params(Type, Vals);
list_params(Type, Vals) ->
   lists:foldl(
      fun
         (Val, Acc) ->
            Acc ++ [list_param(Type, Val)]
      end,
      [],
      Vals
   )
.
list_params({list, Vals}) -> Vals;
list_params(Vals) ->
   lists:foldl(
      fun
         ({_Type, Val}, Acc) ->
            Acc ++ [Val];
         (Val, Acc) ->
            Acc ++ [Val]
      end,
      [],
      Vals
   )
   .

list_param(atom_list, Val) ->
   cparam(atom, Val);
list_param(lambda_list, Val) ->
   cparam(lambda, Val);
list_param(_Type, Val) ->
   cparam(any, Val).

cparam(is_set, _) -> true;
cparam(atom, {identifier, Val}) -> binary_to_atom(Val);
cparam(bool, {identifier, Val}) -> binary_to_atom(Val);
cparam(binary, {string, Val}) -> Val;
cparam(lambda, {lambda, Fun, BinRefs, FunRefs}) -> make_lambda_fun(Fun, FunRefs, BinRefs);
cparam(lambda, Fun) -> Fun;
cparam(list, {_T, Val}) -> [Val];
%%cparam(integer, {_T, Val}) -> Val;
cparam(_, {_Type, Val}) -> Val;
cparam(_, V) -> V.

make_lambda_fun(LambdaString, FunParams, BinRefs) ->
   {Bindings, _Index} = lists:foldl(
      fun(P, {Bindings, Index}) ->
         Bind = bind_lambda_param(lists:nth(Index, FunParams), P),
         {Bindings ++ Bind, Index+1}
      end,
      {"", 1},
      BinRefs
   ),
   F =  "fun(Point) -> " ++ Bindings ++ " fun() -> " ++ LambdaString ++ " end end.",
   lager:notice("lambda: ~p",[F]),
   Fun = parse_fun(F),
   Fun
.

bind_lambda_param(PName, BinRef) ->
%%   lager:notice("*************************************~n bind_lambda: ~p", [{PName, BinRef}]),
   PName ++ " = flowdata:value(Point, <<\"" ++ binary_to_list(BinRef) ++ "\">>), ".


parse_fun(S) ->
   case erl_scan:string(S) of
      {ok, Ts, _} ->
         {ok, Exprs} = erl_parse:parse_exprs(Ts),
         {value, Fun, _} = erl_eval:exprs(Exprs, []),
         Fun;
         {error, ErrorInfo, _ErrorLocation} ->
            Msg = io_lib:format("Error scanning lambda expression: ~p location:~p",
               [ErrorInfo, _ErrorLocation]),
            throw(Msg)

   end.

binary_to_atom(Val) ->
   case catch binary_to_existing_atom(Val, utf8) of
      C when is_atom(C) -> C;
      _ -> lager:error("ATOM :~s not found",[Val]),
         throw("Illegal Option '" ++ binary_to_list(Val) ++ "'")
   end.

-spec node_name(binary()) -> atom().
node_name(Name) when is_binary(Name) ->
   case stat_node(Name) of
      undefined -> NodeName = << <<"esp_">>/binary, Name/binary >>,
         case (catch binary_to_existing_atom(NodeName, utf8)) of
            C when is_atom(C) ->
               C;
            _ ->
               %% check for similar nodes
               SimilarOptions = lists:filter(fun({Dist, _}) -> Dist < 6 end,
                  lev_dist(binary_to_list(Name), get_all_nodes())),
               Add = case SimilarOptions of
                        [] -> "";
                        _ -> ", did you mean: " ++ lists:flatten(
                           lists:join(" or ", ["'" ++ O ++ "'" || {_Dist, O} <- SimilarOptions])) ++ " ?"
                     end,
               throw("Component '" ++ binary_to_list(Name) ++ "' not found" ++ Add)
         end;
      StatNode -> StatNode
   end
   .

-spec get_all_nodes() -> list(string()).
get_all_nodes() ->
   AllNodePaths = filelib:wildcard("lib/faxe-*/ebin/esp_*.beam"),
   [begin
       B0 = string:replace(FileName, "esp_", ""),
       lists:flatten(string:replace(lists:last(B0), ".beam", ""))
    end
      || FileName <- AllNodePaths].


lev_dist(VarName, Opts) ->
   Possibilities = [ begin
                        {faxe_util:levenshtein(VarName, OptName), OptName}
                     end || OptName <- Opts],
   Sorted = lists:sort(Possibilities),
   lists:sublist(Sorted, 3).


stat_node(<<"avg">>) -> esp_avg;
stat_node(<<"bottom">>) -> esp_bottom;
stat_node(<<"count">>) -> esp_count;
stat_node(<<"distinct">>) -> esp_distinct;
stat_node(<<"elapsed">>) -> esp_elapsed;
stat_node(<<"first">>) -> esp_first;
stat_node(<<"geometric_mean">>) -> esp_geometric_mean;
stat_node(<<"kurtosis">>) -> esp_kurtosis;
stat_node(<<"last">>) -> esp_last;
stat_node(<<"max">>) -> esp_max;
stat_node(<<"mean">>) -> esp_mean;
stat_node(<<"median">>) -> esp_median;
stat_node(<<"min">>) -> esp_min;
stat_node(<<"percentile">>) -> esp_percentile;
stat_node(<<"range">>) -> esp_range;
stat_node(<<"skew">>) -> esp_skew;
stat_node(<<"difference">>) -> esp_stats_difference;
stat_node(<<"dist_count">>) -> esp_stats_dist_count;
stat_node(<<"stddev">>) -> esp_stddev;
stat_node(<<"top">>) -> esp_top;
stat_node(<<"variance">>) -> esp_variance;
stat_node(_) -> undefined.
