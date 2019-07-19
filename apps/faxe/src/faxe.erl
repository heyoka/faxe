%% Date: 28.04.17 - 23:17
%% Ⓒ 2017 Alexander Minichmair
-module(faxe).
-author("Alexander Minichmair").

-include("faxe.hrl").
%% API
-export([
   register_task/3,
   register_file_task/2,
   register_string_task/2,
   list_tasks/0,
   start_task/1,
   stop_task/1,
   delete_task/1,
   register_template_file/2,
   register_template_string/2,
   task_from_template/3, task_from_template/2, join/1, join/0, list_templates/0, delete_template/1, start_many/3]).

%%iso1006_header_decode(<<3:8,_Reserved:8, PLength:16/integer-unsigned>>) ->
%%   PLength.



start_many(FileName, TaskName, Num) when is_binary(TaskName), is_integer(Num) ->
   ok = register_template_file(FileName, TaskName),
   start_many(TaskName, Num).
start_many(_TName, 0) -> ok;
start_many(TName, Num) ->
   TaskName = <<TName/binary, (integer_to_binary(Num))/binary>>,
   ok = task_from_template(TName, TaskName),
   start_task(TaskName),
   start_many(TName, Num-1).


join() ->
   faxe_db:db_init().

join(NodeName) ->
   pong = net_adm:ping(NodeName),
   faxe_db:db_init().

-spec list_tasks() -> list().
list_tasks() ->
   faxe_db:get_all_tasks().

list_templates() ->
   faxe_db:get_all_templates().

-spec register_file_task(list()|binary(), any()) -> any().
register_file_task(DfsScript, Name) ->
   register_task(DfsScript, Name, file).

-spec register_string_task(list()|binary(), any()) -> any().
register_string_task(DfsScript, Name) ->
   register_task(DfsScript, Name, data).

register_template_file(DfsFile, TemplateName) ->
   {ok, DfsParams} = application:get_env(faxe, dfs),
   Path = proplists:get_value(script_path, DfsParams),
   lager:info("dfs file path is: ~p",[Path++DfsFile]),
   {ok, Data} = file:read_file(Path++DfsFile),
   StringData = binary_to_list(binary:replace(Data, <<"\\">>, <<>>, [global])),
   register_template_string(StringData, TemplateName).

register_template_string(DfsString, TemplateName) ->
   register_template(DfsString, TemplateName, data).


-spec register_template(list(), term(), atom()) -> 'ok'|{error, template_exists}|{error, not_found}|{error, Reason::term()}.
register_template(DfsScript, Name, Type) ->
   case faxe_db:get_template(Name) of
      {error, not_found} ->
         Def = faxe_dfs:Type(DfsScript, []),
         case Def of
            _ when is_map(Def) ->
               Template = #template{
                  date = faxe_time:now_date(),
                  definition = Def,
                  name = Name,
                  dfs = DfsScript
               },
               faxe_db:save_template(Template);
            {error, What} -> {error, What}
         end;
      _T ->
         {error, template_exists}
   end.

task_from_template(TemplateId, TaskName) ->
   task_from_template(TemplateId, TaskName, []).
task_from_template(TemplateId, TaskName, Vars) ->
   case faxe_db:get_template(TemplateId) of
      {error, not_found} -> {error, template_not_found};
      Template = #template{} -> template_to_task(Template, TaskName, Vars), ok
   end.

-spec register_task(any(), any(), any()) -> any().
register_task(DfsScript, Name, Type) when is_list(DfsScript) ->
   case faxe_db:get_task(Name) of
      {error, not_found} ->
         Def = faxe_dfs:Type(DfsScript, []),
         case Def of
            _ when is_map(Def) ->
               Task = #task{
                  date = faxe_time:now_date(),
                  definition = Def,
                  name = Name
               },
               faxe_db:save_task(Task);
            {error, What} -> {error, What}
         end;
      _T ->
         {error, task_exists}
   end.

start_task(TaskId) ->
   start_task(TaskId, push).
start_task(TaskId, _GraphRunMode) ->
   case faxe_db:get_task(TaskId) of
      {error, not_found} -> {error, task_not_found};
      T = #task{definition = GraphDef, name = Name} ->
         {ok, Graph} = dataflow:create_graph(Name, GraphDef),
%%         ok = dataflow:start_graph(Graph, GraphRunMode),
         faxe_db:save_task(T#task{pid = Graph, last_start = faxe_time:now_date()}),
         {ok, Graph}
   end.

stop_task(TaskId) ->
   T = faxe_db:get_task(TaskId),
   case T of
      {error, not_found} -> ok;
      #task{pid = Graph} when is_pid(Graph) ->
         case is_process_alive(Graph) of
            true ->
               df_graph:stop(Graph);
            false -> ok
         end,
         faxe_db:save_task(T#task{pid = undefined, last_stop = faxe_time:now_date()});
      #task{} -> ok
   end.

delete_task(TaskId) ->
   T = faxe_db:get_task(TaskId),
   case T of
      {error, not_found} -> ok;
      #task{} ->
         stop_task(TaskId),
         faxe_db:delete_task(TaskId)
   end.

delete_template(TaskId) ->
   T = faxe_db:get_template(TaskId),
   case T of
      {error, not_found} -> ok;
      #template{} ->
         faxe_db:delete_template(TaskId)
   end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
template_to_task(#template{dfs = DFS}, TaskName, Vars) ->
   Def = faxe_dfs:data(DFS, Vars),
   case Def of
      _ when is_map(Def) ->
         Task = #task{
            date = faxe_time:now_date(),
            definition = Def,
            name = TaskName},
         faxe_db:save_task(Task);
      {error, What} -> {error, What}
   end.