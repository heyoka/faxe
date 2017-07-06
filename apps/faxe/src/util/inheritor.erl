%% @doc a parse_transform for simple module inheritance
%%
-module(inheritor).
-export([parse_transform/2, do/0]).

do() ->
   {ok, Data} = epp:parse_file("/home/heyoka/workspace/faxe/apps/faxe/src/components/esp_stats.erl",[]),
   parse_transform(Data,[]).

parse_transform(AST,_Opts) ->
%%   io:format("OPTS: ~p~n",[AST]),
   Functions = [{Func, Arity} || {function, _, Func, Arity,_} <- AST],
   walk_ast(AST,[], false, Functions).

walk_ast([], [H|NewAST], _, Functions) ->
   {eof,Line} = H,
   case(get(import)) of
      undefined ->
         lists:reverse(NewAST);
      Module ->
         code:ensure_loaded(Module),
         case(erlang:module_loaded(Module)) of
            true ->
               Line1 = Line + 1,
               Importable = lists:filter(fun ({module_info,_}) -> false; (_)->true end, Module:module_info(exports)),
               {Imported, LastLine}  = handle_import(Module, Line1, Importable, Functions),
               Ast = lists:reverse([{eof, LastLine}| Imported]),
%%               io:format("FUNCTIONS ~p ~n M_FUNCTIONS: ~p ~n",[Functions, get_functions(Ast)]),
               {TrimmedAST, ExportList} = get_exports(Ast),
               FinalAST = lists:flatten(lists:reverse([TrimmedAST|NewAST])),
               MergedExportsAST = merge_exports(ExportList, FinalAST),
               MergedExportsAST;
            _ -> erlang:exit(io:format("Unable to inherit from module '~p'. "++
               "Reason: Unable to locate '~p' module. Is it loaded?~n", [Module, Module]))
         end
   end;
walk_ast([{attribute, _, inherit, Module}| RestAST], NewAST, Inherited, Functions) ->
   if
      Inherited == true -> erlang:exit(io:format("Unable to inherit from module '~p'. "++
                           "Reason: Already inheriting from a module.~n", [Module]));
      true ->
         put(import, Module),
         walk_ast(RestAST, NewAST, true, Functions)
   end;
walk_ast([Node|ASTRest], NewAST, Inherited, Functions) -> walk_ast(ASTRest, [Node|NewAST], Inherited, Functions).

handle_import(Module, Line, Possibles, Functions)->
   handle_import(Module, Line, Possibles, Functions, []).

%handle_import(_, [], _, []) -> [];
handle_import(Module,Line, [], _, Importable) ->
   Exports = build_export_statements(Importable),
   if
      length(Exports) > 0 ->
         PS = lists:flatten(["-export([", Exports, "]).", io_lib:format("~n",[])]),
         PS1 = lists:flatten(PS ++ build_functions(atom_to_list(Module), Importable)),
         AST = parse(PS1,Line),
         AST;
      true -> {[],0}
   end;

handle_import(Module, Line, [Possible|T], Functions, Actual) ->
   case (importable(Possible, Functions)) of
      true-> Actual1 = [Possible|Actual];
      false-> Actual1 = Actual
   end,
   handle_import(Module,Line, T,Functions, Actual1).

parse(String, Line) -> parse(String, Line,[]).
parse(String, Line, ASTOut) ->
   {done,{ok,Tokens,_},StringRest} = erl_scan:tokens([], String, Line),
   {ok, AST} = erl_parse:parse_form(Tokens),
   case (StringRest) of
      [] -> {[AST|ASTOut], Line};
      _ -> parse(StringRest, Line + 1, [AST|ASTOut])
   end.

importable(_,[])-> true;
importable(Function, [Function|_]) -> false;
importable(Function,[_|T]) -> importable(Function, T).

build_export_statements([])->"";
build_export_statements([{Func, Arity}|[]]) -> io_lib:format("~p\/~p",[Func, Arity]);
build_export_statements([{Func,Arity}|T]) -> io_lib:format("~p\/~p,",[Func, Arity]) ++ build_export_statements(T).

build_functions(Module, [Function|[]]) -> build_function(Module, Function);
build_functions(Module, [Function|T]) -> build_function(Module, Function) ++ build_functions(Module,T).

build_function(Module, {Name, Arity}) ->
   ArgList = lists:flatten(build_arg_list(Arity)),
   FName = atom_to_list(Name),
   Function = lists:flatten([FName, "(", ArgList, ") -> ", Module, ":", FName,"(", ArgList, "). "]),
%%   io:format("built function: ~p~n",[Function]),
   Function.

build_arg_list(N) when N < 1 -> "";
build_arg_list(N) when N == 1 -> io_lib:format("Arg~p", [N]);
build_arg_list(N) -> io_lib:format("Arg~p,", [N]) ++ build_arg_list(N - 1).


get_exports(Ast) -> get_exports(Ast,[]).

get_exports([], AST) -> {AST, []};
get_exports([{attribute, _, export, ExportList}|T], AST) -> {[AST|T], ExportList};
get_exports([Node|T], AST) -> get_exports(T,[AST|Node]).


%%get_functions(Ast) -> get_functions(Ast, []).
%%
%%get_functions([], Ast) -> {Ast, []};
%%get_functions([{function, _, FName, FArity, _}|T], Ast) -> {[Ast|T], {FName, FArity}};
%%get_functions([Node|T], AST) -> get_functions(T,[AST|Node]).



merge_exports(ExportList, AST) ->
   {value, {_,Line,_,Exports}} = lists:keysearch(export,3, AST),
   lists:keyreplace(export,3, AST, {attribute, Line, export, Exports ++ ExportList}).