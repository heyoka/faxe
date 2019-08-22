%% Date: 28.04.17 - 23:17
%% faxe api
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
   start_task/2,
   stop_task/1,
   stop_task/2,
   delete_task/1,
   register_template_file/2,
   register_template_string/2,
   task_from_template/3,
   task_from_template/2,
   join/1, join/0,
   list_templates/0,
   delete_template/1,
   start_many/3,
   list_running_tasks/0,
   start_permanent_tasks/0]).

%%iso1006_header_decode(<<3:8,_Reserved:8, PLength:16/integer-unsigned>>) ->
%%   PLength.

start_permanent_tasks() ->
   Tasks = faxe_db:get_permanent_tasks(),
   [start_task(T, true) || T <- Tasks].

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

list_running_tasks() ->
   Graphs = supervisor:which_children(graph_sup),
   faxe_db:get_tasks_by_pids(Graphs).

-spec register_file_task(list()|binary(), any()) -> any().
register_file_task(DfsScript, Name) ->
   register_task(DfsScript, Name, file).

-spec register_string_task(list()|binary(), any()) -> any().
register_string_task(DfsScript, Name) ->
   register_task(DfsScript, Name, data).

-spec register_task(list()|binary(), binary(), atom()) -> ok|{error, task_exists}| {error, term()}.
register_task(DfsScript, Name, Type) ->
   case faxe_db:get_task(Name) of
      {error, not_found} ->
         try faxe_dfs:Type(DfsScript, []) of
            Def when is_map(Def) ->
               Task = #task{
                  date = faxe_time:now_date(),
                  definition = Def,
                  name = Name
               },
               faxe_db:save_task(Task);

            {error, What} -> {error, What}
         catch
            throw:Err -> {error, Err};
            exit:Err -> {error, Err};
            error:Err -> {error, Err};
            _:_      -> {error, unknown}
         end;

      _T ->
         {error, task_exists}
   end.

register_template_file(DfsFile, TemplateName) ->
   {ok, DfsParams} = application:get_env(faxe, dfs),
   Path = proplists:get_value(script_path, DfsParams),
%%   lager:info("dfs file path is: ~p",[Path++DfsFile]),
   {ok, Data} = file:read_file(Path++DfsFile),
   StringData = binary_to_list(binary:replace(Data, <<"\\">>, <<>>, [global])),
   register_template_string(StringData, TemplateName).

-spec register_template_string(list(), binary()) ->
   'ok'|{error, template_exists}|{error, not_found}|{error, Reason::term()}.
register_template_string(DfsString, TemplateName) ->
   register_template(DfsString, TemplateName, data).


-spec register_template(list(), term(), atom()) ->
   'ok'|{error, template_exists}|{error, not_found}|{error, Reason::term()}.
register_template(DfsScript, Name, Type) ->
   case faxe_db:get_template(Name) of
      {error, not_found} ->

         try faxe_dfs:Type(DfsScript, []) of
            Def when is_map(Def) ->
               Template = #template{
                  date = faxe_time:now_date(),
                  definition = Def,
                  name = Name,
                  dfs = DfsScript
               },
               faxe_db:save_template(Template);

            {error, What} -> {error, What}

         catch
            throw:Err -> {error, Err};
            exit:Err -> {error, Err};
            error:Err -> {error, Err};
            _:_      -> {error, unknown}
         end;

      _T ->
         {error, template_exists}
   end.

task_from_template(TemplateId, TaskName) ->
   task_from_template(TemplateId, TaskName, []).
task_from_template(TemplateId, TaskName, Vars) ->
   case faxe_db:get_task(TaskName) of
      {error, not_found} -> case faxe_db:get_template(TemplateId) of
                               {error, not_found} -> {error, template_not_found};
                               Template = #template{} -> template_to_task(Template, TaskName, Vars), ok
                            end;
      #task{} -> {error, task_exists}
   end.


start_task(TaskId) ->
   start_task(TaskId, false).
start_task(TaskId, Permanent) ->
   start_task(TaskId, push, Permanent).
-spec start_task(integer()|binary(), atom(), true|false) -> any().
start_task(TaskId, _GraphRunMode, Permanent) ->
   case faxe_db:get_task(TaskId) of
      {error, not_found} -> {error, task_not_found};
      T = #task{definition = GraphDef, name = Name} ->
         case dataflow:create_graph(Name, GraphDef) of
            {ok, Graph} ->
               faxe_db:save_task(T#task{pid = Graph,
                  last_start = faxe_time:now_date(), permanent = Permanent}),
               {ok, Graph};
            {error, {already_started, _Pid}} -> {error, already_started}
         end
   end.


-spec stop_task(integer()|binary()) -> ok.
stop_task(TaskId) ->
   stop_task(TaskId, false).
-spec stop_task(integer()|binary(), true|false) -> ok.
stop_task(TaskId, Permanent) ->
   T = faxe_db:get_task(TaskId),
   case T of
      {error, not_found} -> ok;
      #task{pid = Graph} when is_pid(Graph) ->
         case is_process_alive(Graph) of
            true ->
               df_graph:stop(Graph);
            false -> ok
         end,
         NewT =
         case Permanent of
            true -> T#task{permanent = false};
            false -> T
         end,
         faxe_db:save_task(NewT#task{pid = undefined, last_stop = faxe_time:now_date()});
      #task{} -> ok
   end.

-spec delete_task(term()) -> ok | {error, not_found} | {error, task_is_running}.
delete_task(TaskId) ->
   T = faxe_db:get_task(TaskId),
   case T of
      {error, not_found} -> {error, not_found};
      #task{pid = Graph} when is_pid(Graph) ->
         case is_process_alive(Graph) of
            true -> {error, task_is_running};
            false -> faxe_db:delete_task(TaskId)
         end;
      #task{} -> faxe_db:delete_task(TaskId)
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