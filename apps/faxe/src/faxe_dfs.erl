%% Date: 23.02.17 - 20:24
%% Ⓒ 2017 heyoka
-module(faxe_dfs).
-author("Alexander Minichmair").

-include("faxe.hrl").

%% API
-export([start_script/2, do/0, do/1, do/2, compile/1, file/2, data/2, make_lambda_fun/3]).

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



file(ScriptFile, Vars) when is_list(ScriptFile), is_list(Vars) ->
   {ok, DfsParams} = application:get_env(faxe, dfs),
   Path = proplists:get_value(script_path, DfsParams),
%%   lager:info("dfs file path is: ~p",[Path++ScriptFile]),
   D = dfs:parse_file(Path ++ ScriptFile, ?LAMBDA_LIBS, Vars),
   maybe_compile(D).

data(DfsData, Vars) ->
   D = dfs:parse(DfsData, ?LAMBDA_LIBS, Vars),
   maybe_compile(D).

maybe_compile(ParserResult) ->
   case ParserResult of
      {{_Where, line, _LN}, {Keyword, MsgList}} = _M ->
         {error, iolist_to_binary(
            [atom_to_binary(_Where), <<" on line ">>,
               integer_to_binary(_LN), <<": ">>,
               atom_to_binary(Keyword), <<" ">>, list_to_binary(MsgList)])
         };
      {{_Where, line, _LN}, _Message} = _M ->
         {error, iolist_to_binary(
            [atom_to_binary(_Where), <<" on line ">>,
               integer_to_binary(_LN), <<": ">>, list_to_binary(_Message)])
         };
      {_Nodes, _Connections} -> compile(ParserResult);
      _ -> {error, ParserResult}
   end.

compile(D) ->
   try eval(D) of
      GraphDef when is_map(GraphDef) ->
         GraphDef;
      Err -> {error, Err}
   catch
      _:Err -> {error, Err}
   end.


-spec eval(tuple()) -> map().
eval({Nodes, Connections}) ->

   Def = dataflow:new_graph(),

   %% add nodes, handle node options and parameters and build connections
   {NewDef, NewConnections} =
      lists:foldl(
         fun({{NodeName, _Id} = N, Params, Options}, {Def0, Conns}) ->
            {Component, NOpts} =
            case NodeName of
               << ?USER_NODE_PREFIX, Callback/binary>> ->
                  Class = estr:str_capitalize(Callback),
                  {?USER_COMPONENT,
                     [
                        {?USER_COMPONENT_MODULE,list_to_atom(binary_to_list(Callback))},
                        {?USER_COMPONENT_CLASS, list_to_atom(binary_to_list(Class))}
                     ]
                  };
               _ ->
                  {node_name(NodeName), []}
            end,

            %% build additional connections from node-options
            {NewConns, ParamOptions} = node_conn_params(N, Params),

            CompOptions0 =
            case Component of
               ?USER_COMPONENT -> case erlang:function_exported(?USER_COMPONENT, call_options, 2) of
                              true ->
                                 Callb = proplists:get_value(?USER_COMPONENT_MODULE, NOpts),
                                 ClassName = proplists:get_value(?USER_COMPONENT_CLASS, NOpts),
                                 ?USER_COMPONENT:call_options(Callb, ClassName);
                              false -> []
                           end;
               _        ->  case erlang:function_exported(Component, options, 0) of
                               true -> Component:options();
                               false -> throw("Component '" ++ binary_to_list(NodeName) ++ ";options' not found")
                            end
            end,
            %% add ls_mem options as optional
            LsMem = [{ls_mem, binary, undefined}, {ls_mem_field, binary, <<>>}, {ls_mem_ttl, integer, 0}],
            CompOptions = CompOptions0 ++ LsMem,
            %% handle all other params and options
            NOptions = convert_options(CompOptions, lists:flatten(Options ++ ParamOptions)),
            NodeOptions = NOptions ++ NOpts,
            lager:notice("~n~p wants options : ~p~n has options: ~p~n~n NodeParameters: ~p",
               [Component, Component:options(), Options ++ ParamOptions, NodeOptions]),

            %% check options with the components option definition
            %% any errors raised here, would be caught in the surrounding call
            dataflow:build_options(Component, NodeOptions),

            NodeId = node_id(N),
            lager:info("all options for node ~p: ~p", [N, NodeOptions]),
%%            dataflow:maybe_check_opts(Component, Options ++ ParamOptions),

            {
               dataflow:add_node({NodeId, Component, NodeOptions}, Def0),
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

%%eval_node({<< "@", Name/binary >>, Id}) ->
%%   {node_name(Name), node_id({Name, Id}), []};
%%eval_node({{NodeName, _Id} = N, Params, Options}) ->
%%   {node_name(NodeName), node_id(N), []}.

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
                        [{ParamName, [{lambda, Fun}]}|Params0];

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
               case catch(lists:nth(Index, CParams)) of
                  {all, NPort} -> [{N, CNode, NPort}|Cs];


                  {Index, NPort} -> [{N, CNode, NPort}|Cs];

                  _Error ->
                     throw("Illegal or missing Connection Parameter " ++
                     integer_to_list(Index) ++ " for node '" ++ binary_to_list(NodeName)++"'" )
               end
               ,
               {NewConnections, Params, Index+1};
            (Par, {Cs0, Params0, Index0}) -> {Cs0, [Par]++Params0, Index0+1}
         end,
         {[], [], 1},
         NodeParams
      ),
   NewNodeParams = node_params(N, Parameters),
   {NewConns, NewNodeParams}.

-spec convert_options(list(), list()) -> list({binary(),list()}).
convert_options(NodeOptions, Params) ->
%%   lager:warning("convert options: ~p~n~p", [NodeOptions, Params]),
   Opts = lists:foldl(
      fun
         ({Name, Type, _Def}, O) -> [{atom_to_binary(Name), {Name, Type}}|O];
         ({Name, Type}, O)       -> [{atom_to_binary(Name), {Name, Type}}|O];
         %% ignore port connectors
         (_, O)                  -> O
      end,
      [],
      NodeOptions
   ),
%%   lager:notice("Options for Node: ~p",[Opts]),
   lists:foldl(
      fun
         ({PName, PVals}, Acc) ->
%%            lager:warning("~p :: ~p~n",[PName, proplists:get_value(PName, Opts)]),
         case proplists:get_value(PName, Opts) of
            undefined ->
               lager:warning("type is: ~p",[{PName, PVals}]),
               Acc;
            {Name, param_list = Type} ->
               {value, {Name, Type, POpts}, _L} = lists:keytake(Name, 1, NodeOptions),
%%               lager:warning("~nconvert param_list(~p, ~p, ~p, ~p)",[Name, Type, PVals, POpts]),
               Zipped = lists:zip(POpts, PVals),
               C = [convert(N, T, [PV]) || {{N, T}, PV} <- Zipped],
               [{Name, C} | Acc];
            {Name, Type} ->
%%               lager:info("~nconvert(~p, ~p, ~p)",[Name, Type, PVals]),
               [convert(Name, Type, PVals) | Acc]

         end
      end,
      [],
      Params

   ).

%%convert(Name, param_list, PVals) ->
%%   {Name, [convert(N, T, PVal) || {N, T}]}
%%   lists:map(fun({N, Typ, PVal}) -> convert(N, Typ, PVal) end, Type);
convert(Name, Type, PVals) ->
%%   lager:notice("convert(~p,~p,~p)",[Name, Type, PVals]),
   TName = atom_to_binary(Type),
   case estr:str_ends_with(TName, <<"list">>) of
      true -> {Name, list_params(Type, PVals)};
      false -> case length(PVals) of
                  0 -> {Name, cparam(Type, [])};
                  1 -> {Name, cparam(Type, hd(PVals))};
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
cparam(binary, {string, Val}) -> Val;
cparam(lambda, {lambda, Fun, BinRefs, FunRefs}) -> make_lambda_fun(Fun, FunRefs, BinRefs);
cparam(lambda, Fun) -> Fun;
cparam(list, {_T, Val}) -> [Val];
%%cparam(integer, {_T, Val}) -> Val;
cparam(_, {_Type, Val}) -> Val;
cparam(_, V) -> V.

make_lambda_fun(LambdaString, FunParams, BinRefs) ->
%%   lager:warning("~~n make_lambda_fun(~p, ~p, ~p)~n",[LambdaString, FunParams, BinRefs]),
   {Bindings, _Index} = lists:foldl(
      fun(P, {Bindings, Index}) ->
         Bind = bind_lambda_param(lists:nth(Index, FunParams), P),
         {Bindings ++ Bind, Index+1}
      end,
      {"", 1},
      BinRefs
   ),
   F =  "fun(Point) -> " ++ Bindings ++ " fun() -> " ++ LambdaString ++ " end end.",
%%   lager:warning("~nfun: ~p~n",[F]),
   Fun = parse_fun(F),
   Fun
.

bind_lambda_param(PName, BinRef) ->
%%   lager:notice("*************************************~n bind_lambda: ~p", [{PName, BinRef}]),
   PName ++ " = flowdata:value(Point, <<\"" ++ binary_to_list(BinRef) ++ "\">>), ".


parse_fun(S) ->
   lager:notice("parse_fun(~p)",[S]),
   {ok, Ts, _} = erl_scan:string(S),
   lager:warning("parse tokens: ~p",[Ts]),
   {ok, Exprs} = erl_parse:parse_exprs(Ts),
   {value, Fun, _} = erl_eval:exprs(Exprs, []),
   Fun.

binary_to_atom(Val) ->
   case (catch binary_to_existing_atom(Val, utf8)) of
      C when is_atom(C) -> C;
      _ -> throw("Illegal Option '" ++ binary_to_list(Val) ++ "'")
   end.

-spec node_name(binary()) -> atom().
node_name(Name) when is_binary(Name) ->
   case stat_node(Name) of
      undefined -> NodeName = << <<"esp_">>/binary, Name/binary >>,
         case (catch binary_to_existing_atom(NodeName, utf8)) of
            C when is_atom(C) ->
               C;
            _ -> throw("Component '" ++ binary_to_list(NodeName) ++ "' not found")
         end;
      StatNode -> StatNode
   end
   .

atom_to_binary(V) ->
   list_to_binary(atom_to_list(V)).

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
