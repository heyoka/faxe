-module(dataflow).

-include("faxe.hrl").

%% API exports
-export([new_graph/0, create_graph/2, start_graph/1, start_graph/2,
   add_node/2, add_edge/2, add_debug_handler/0, remove_debug_handler/0]).

-export([request_items/2, emit/1, build_options/3, maybe_check_opts/2]).

%%====================================================================
%% CALLBACK API functions
%%====================================================================

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

add_debug_handler() ->
   dataflow_events:add_handler(dfevent_graph),
   dataflow_events:add_handler(dfevent_component).

remove_debug_handler() ->
   dataflow_events:add_handler(dfevent_graph),
   dataflow_events:add_handler(dfevent_component).

%% @doc get a new graph definition map
-spec new_graph() -> graph_definition().
new_graph() ->
   #{nodes => [], edges => []}.

%% @doc graph_definition() : add graph node
-spec add_node({any(), atom()} | {any(), atom(), list()}, graph_definition()) -> graph_definition().
add_node({NodeName, Component}, Defs) when is_atom(Component), is_map(Defs) ->
   add_node({NodeName, Component, []}, Defs);
add_node({NodeName, Component, Params}, Defs=#{nodes := Nodes})
      when is_atom(Component), is_map(Defs), is_map(Params) ->
   Defs#{nodes := [{NodeName, Component, Params} | Nodes]}.

%% @doc add a new edge to #{nodes := Nodes, edges := Edges}
-spec add_edge(tuple(), graph_definition()) -> graph_definition().
add_edge({NodeOut, PortOut, NodeIn, PortIn}, Defs) ->
   add_edge({NodeOut, PortOut, NodeIn, PortIn, []}, Defs);
add_edge({NodeOut, PortOut, NodeIn, PortIn, Params}, Defs = #{edges := Edges}) ->
   Defs#{edges := [{NodeOut, PortOut, NodeIn, PortIn, Params} | Edges]}.

%% @doc start a new df_graph process
-spec create_graph(any(), graph_definition()) -> {ok, pid()} | {error, Reason::any()}.
create_graph(Id, Definitions) when is_map(Definitions) ->
   lager:debug("start/create graph: ~p", [Id]),
   graph_sup:new(Id, Definitions).
%%   create_graph(Id, Definitions, 1).
%create_graph(Id, Definitions, _RepFactor) when is_list(Id), is_map(Definitions) ->
%   'Elixir.Swarm':'register_name'(Id, graph_sup, new, [Id, Definitions]).

%% @doc start the graph computation
start_graph(Graph) ->
   start_graph(Graph, push).
start_graph(Graph, #task_modes{} = TM) ->
   df_graph:start_graph(Graph, TM#task_modes{run_mode = push});
start_graph(Graph, RunMode) ->
   df_graph:start_graph(Graph, #task_modes{run_mode = RunMode}).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% component functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
request_items(Port, PublisherPids) when is_list(PublisherPids) ->
   [Pid ! {request, self(), Port} || Pid <- PublisherPids].

emit(Value) ->
   emit(1, Value).
emit(Port, Value) ->
   erlang:send_after(0, self(), {emit, {Port, Value}}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec build_options(atom(), list( {atom(), option_value()} ), map()) -> map().
build_options(Component, L, Opts) ->
%%   lager:notice("build options for: ~p :: ~p -------------- ~p", [Component, L, Opts]),
   case catch(do_build_options(Opts, L)) of
      Opts0 when is_map(Opts0) -> maybe_check_opts(Opts0, Component);
      {error, What} -> erlang:error({bad_option, {Component, What}});
      {'EXIT',{What, _}} -> erlang:error({bad_option, {Component, What}});
      Error -> erlang:error(format_error(bad_option, Component, Error))
   end.
do_build_options([], _) -> #{};
do_build_options(Opts, L) when is_list(L), is_list(Opts) ->
   lists:foldl(
      fun
         ({OptName, is_set}, Acc) ->
            case proplists:get_value(OptName, L) of
               undefined -> Acc#{OptName => false};
               true      -> Acc#{OptName => true}
            end;
         ({OptName, is_set, Default}, Acc) ->
            case proplists:get_value(OptName, L) of
               undefined -> Acc#{OptName => Default};
               true      -> Acc#{OptName => true}
            end;
         ({OptName, OptType, Default}, Acc) ->
            case proplists:get_value(OptName, L) of
               undefined -> Acc#{OptName => Default};
               V        -> Acc#{OptName => val(V, {OptName, OptType})}
            end;
         ({OptName, OptType}, Acc) ->
            case proplists:get_value(OptName, L) of
               undefined -> throw([<<"option_missing: '">>,
                  atom_to_binary(OptName,utf8), <<"', required type: ">>, atom_to_binary(OptType,utf8)]);
               V        -> Acc#{OptName => val(V, {OptName, OptType})}
            end
      end,
      #{},
      Opts).

%%====================================================================
%% Internal functions
%%====================================================================

%% @todo convert types accordingly, ie: string(binary) to real string(list)
-spec val(option_value(), {Name :: binary(), option_name()}) -> option_value().
val(Val, {OptName, duration}) when is_binary(Val) ->
   case catch(faxe_time:duration_to_ms(Val)) of
      T when is_integer(T) -> Val;
      _ -> option_error(<<"bad parameter type">>, Val, duration, OptName)
   end;
val(Val, {_, number}) when is_integer(Val) orelse is_float(Val) -> Val;
val(Val, {_, integer}) when is_integer(Val) -> Val;
val(Val, {_, float}) when is_float(Val) -> Val;
val(Val, {_, double}) when is_float(Val) -> Val;
val(Val, {_, binary}) when is_binary(Val) -> Val;
val(Val, {_, string}) when is_binary(Val) -> Val;
val(Val, {_, list}) when is_list(Val) -> Val;
val(Val, {_, atom}) when is_atom(Val) -> Val;
val(Val, {_, lambda}) when is_function(Val) -> Val;
val(true, {_, bool}) -> true;
val(false, {_, bool}) -> false;

val(Val, {N, number_list}) when is_list(Val) ->
   list_val(Val, fun(E) -> is_integer(E) orelse is_float(E) end, numbers, N);
val(Val, {N, integer_list}) when is_list(Val) ->
   list_val(Val, fun(E) -> is_integer(E) end, integers, N);
val(Val, {N, float_list}) when is_list(Val) ->
   list_val(Val, fun(E) -> is_float(E) end, floats, N);
val(Val, {N, binary_list}) ->
   val(Val, {N, string_list});
val(Val, {N, string_list}) when is_list(Val) ->
   list_val(Val, fun(E) -> is_binary(E) end, strings, N);
val(Val, {N, atom_list}) when is_list(Val) ->
   list_val(Val, fun(E) -> is_atom(E) end, atoms, N);
val(Val, {N, lambda_list}) when is_list(Val) ->
   list_val(Val, fun(E) -> is_function(E) end, lambdas, N);

val(Val, {_, any}) -> Val;
val(V, {OptName, Type}) -> option_error(<<"bad parameter type">>, V, Type, OptName).

list_val([], _Fun, Type, OptName) ->
   option_error(<<"empty parameter(s)">>, "", Type, OptName);
list_val(Val, Fun, Type, OptName) ->
   case lists:all(Fun, Val) of
      true -> Val;
      false -> option_error(<<"bad parameter(s) type">>, Val, Type, OptName)
   end.


%% further options checks
maybe_check_opts(Opts, Module) when is_map(Opts), is_atom(Module) ->
   case erlang:function_exported(Module, check_options, 0) of
      true -> check_options(Module:check_options(), Opts, Module);
      false -> Opts
   end.

check_options([], Opts, _Mod) ->
   Opts;
check_options([Check| Checks], Opts, Mod) ->
   do_check(Check, Opts, Mod),
   check_options(Checks, Opts, Mod).

do_check({same_length, [Key1|Keys]=_OptionKeys}, Opts = #{}, Mod) ->
%%   lager:warning("same_length check for ~p ~p out of ~p" ,[OptionKeys, Opts]),
   #{Key1 := L1} = Opts,
   L = erlang:length(L1),
   F = fun(KeyE) ->
      case maps:get(KeyE, Opts, undefined) of
         undefined -> ok;
         ListOpts ->
            case erlang:length(ListOpts) == L of
               true -> ok;
               false -> erlang:error(format_error(options_error, Mod,
                  [<<"Different parameter count for options '">>, atom_to_binary(Key1, utf8),
                     <<"' and '">>, atom_to_binary(KeyE, utf8), <<"'">>]))
            end
      end
      end,
   lists:foreach(F, Keys);

do_check({not_empty, Keys}, Opts, Mod) ->
   F = fun(KeyE) ->
      case maps:get(KeyE, Opts, undefined) of
         undefined -> ok; %% should not happen
         [] -> erlang:error(format_error(option_empty, Mod,
            [<<"Option may not be empty: '">>, atom_to_binary(KeyE, utf8), <<"'">>]));
         _ -> ok
      end
      end,
   lists:foreach(F, Keys);

do_check({max_param_count, Keys, Max}, Opts, Mod) ->
   F = fun(KeyE) ->
         case maps:get(KeyE, Opts, undefined) of
            undefined -> ok; %% should not happen
            Params -> case erlang:length(Params) > Max of
                         true -> erlang:error(format_error(too_many_items, Mod,
                            [<<"Max param count: '">>, Max, <<" for ">>,
                               atom_to_binary(KeyE, utf8), <<"'">>]));
                         false -> ok
                      end
         end
       end,
   lists:foreach(F, Keys);

do_check({one_of, Key, ValidOpts}, Opts, Mod) ->
   lager:notice("do check: ~p",[[{one_of, Key, ValidOpts}, Opts, Mod]]),
   case maps:get(Key, Opts, undefined) of
      undefined -> ok; %% should not happen
      Params ->
         lists:foreach(fun(E) ->
                        case lists:member(E, ValidOpts) of
                           true -> ok;
                           false -> erlang:error(format_error(invalid_opt, Mod,
                              [<<"Cannot use '">>, E, <<"' for param '">>, atom_to_binary(Key, latin1),
                                 <<"'">>, <<" must be one of: ">>, ValidOpts]))
                        end
                       end, Params)
   end.

option_error(OptType, Given, Should, Name) ->
   throw([OptType,
      <<" given for param '">>,atom_to_binary(Name, utf8),<<"' ('">>,
      io_lib:format("~w",[Given]), <<"'), should be: ">>, atom_to_binary(Should, utf8)]).

format_error(Type, Component, Error) ->
   iolist_to_binary(
   [atom_to_binary(Type, utf8), <<" for node ">>, atom_to_binary(Component, utf8), <<": ">>] ++ Error
   ).