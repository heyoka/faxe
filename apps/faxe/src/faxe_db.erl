%% Date: 28.04.17 - 20:09
%% Ⓒ 2017 heyoka
-module(faxe_db).
-author("Alexander Minichmair").
-include("faxe.hrl").
-include_lib("stdlib/include/ms_transform.hrl").

-define(WAIT_FOR_TABLES, 10000).

%% API

%% db management
-export([renew_tables/0, create/0, db_init/0, export/1]).

%% api export
-export([
   get_task/1,
   save_task/1,
   delete_task/1,
   get_all_tasks/0,
   get_all_templates/0,
   save_template/1,
   get_template/1,
   delete_template/1,
   get_tasks_by_pids/1,
   get_permanent_tasks/0,
   get_tasks_by_template/1
   , get_tasks_by_ids/1]).

-export([
   add_tags/2,
   remove_tags/2,
   get_all_tags/0,
   get_tasks_by_tag/1
   , get_tasks_by_tags/1
]).

get_all_tasks() ->
   get_all(task).

get_all_templates() ->
   get_all(template).

get_all(Table) ->
   lists:flatten([mnesia:dirty_read(Table, Key) || Key <- mnesia:dirty_all_keys(Table)]).

get_task(TaskId) when is_integer(TaskId) ->
   case mnesia:dirty_read(task, TaskId) of
      [#task{definition = Def} = T] -> T#task{definition = maps:from_list(Def)};
      [] -> {error, not_found}
   end;
get_task(TaskName) ->
   case mnesia:dirty_index_read(task, TaskName, #task.name) of
      [#task{definition = Def} = T] -> T#task{definition = maps:from_list(Def)};
      [] -> {error, not_found}
   end.

get_template(TId) when is_integer(TId) ->
   case mnesia:dirty_read(template, TId) of
      [#template{definition = Def} = T] -> T#template{definition = maps:from_list(Def)};
      [] -> {error, not_found}
   end;
get_template(TemplateName) ->
   case mnesia:dirty_index_read(template, TemplateName, #template.name) of
      [#template{definition = Def} = T] -> T#template{definition = maps:from_list(Def)};
      [] -> {error, not_found}
   end.


get_tasks_by_ids(IdList) ->
   Specs =
      lists:map(
         fun(Id) ->
            {#task{id = '$1',name = '$2',dfs = '_',definition = '_',
               date = '_',pid = '_',last_start = '_',last_stop = '_',
               permanent = '_',is_running = '_',template_vars = '_',
               template = '_', tags = '_'},
               [{'orelse', {'==', '$1', Id}, {'==', '$2', Id}}],
               ['$_']}

         end,
         IdList
      ),
   mnesia:dirty_select(task, Specs).



get_tasks_by_pids(PidList) ->
   lists:flatten([mnesia:dirty_index_read(task, Pid, #task.pid) || {_Name, Pid, _, _} <- PidList]).

get_tasks_by_template(TemplateName) ->
   mnesia:dirty_index_read(task, TemplateName, #task.template).

get_permanent_tasks() ->
   mnesia:dirty_index_read(task, true, #task.permanent).

save_template(#template{id = undefined, definition = Def} = Template) ->
   mnesia:dirty_write(Template#template{id = next_id(template), definition = maps:to_list(Def)});
save_template(#template{definition = Def} = T) ->
   mnesia:dirty_write(T#template{definition = maps:to_list(Def)}).

save_task(#task{id = undefined, definition = Def} = Task) ->
   mnesia:dirty_write(Task#task{id = next_id(task), definition = maps:to_list(Def)});
save_task(#task{definition = Def} = T) ->
   mnesia:dirty_write(T#task{definition = maps:to_list(Def)}).

delete_task(#task{id = Key}) ->
   mnesia:dirty_delete({task, Key});
delete_task(TaskId) ->
   case get_task(TaskId) of
      {error, not_found} -> {error, not_found};
      T = #task{} -> delete_task(T)
   end
.

delete_template(#template{id = Key}) ->
   mnesia:dirty_delete({template, Key});
delete_template(TId) ->
   case get_template(TId) of
      {error, not_found} -> {error, not_found};
      T = #template{} -> delete_template(T)
   end
.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%% Tags %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% get a list of all used tags
get_all_tags() ->
   mnesia:dirty_all_keys(tag_tasks).

get_tasks_by_tag(Tag) ->
   case get_tag_tasks_by_tag(Tag) of
      [] -> [];
      [#tag_tasks{tasks = Tasks}] -> get_tasks_by_ids(Tasks)
   end.

get_tasks_by_tags(Tags) ->
   sets:to_list(sets:from_list(get_tasks_by_tags(Tags, []))).

get_tasks_by_tags([], Acc) ->
   Acc;
get_tasks_by_tags([Tag|Tags], Acc) ->
   NewAcc = get_tasks_by_tag(Tag) ++ Acc,
   get_tasks_by_tags(Tags, NewAcc).

get_tag_tasks_by_tag(Tag) ->
   mnesia:dirty_read(tag_tasks, Tag).


-spec add_tags(any(), list(binary())) -> ok | {error, task_not_found}.
add_tags(TaskId, Tags) when is_list(Tags) ->
   Task = get_task(TaskId),
   case Task of
      #task{} ->
         TaskTags = Task#task.tags,
         NewTags =
            case TaskTags of
               [] -> Tags;
               _ ->
                  AddTags = lists:filter(fun(E) -> not lists:member(E, TaskTags) end, Tags),
                  TaskTags ++ AddTags
            end,
         save_task(Task#task{tags = NewTags}),
         [add_task_to_tag(TaskId, Tag) || Tag <- Tags],
         ok;
      _ -> {error, task_not_found}
   end.


add_task_to_tag(TaskId, Tag) ->
   TagTasks = get_tag_tasks_by_tag(Tag),
   case TagTasks of
      [] -> mnesia:dirty_write(tag_tasks, #tag_tasks{tag = Tag, tasks = [TaskId]});
      [Tsks] ->
         case lists:member(TaskId, Tsks#tag_tasks.tasks) of
            true -> ok;
            false -> mnesia:dirty_write(tag_tasks,
               #tag_tasks{tag = Tag, tasks = [TaskId|Tsks#tag_tasks.tasks]})
         end
   end.

-spec remove_tags(any(), list(binary())) -> ok | {error, task_not_found}.
remove_tags(TaskId, Tags) when is_list(Tags) ->
   Task = get_task(TaskId),
   case Task of
      #task{} ->
         NewTags = Task#task.tags -- Tags,
         save_task(Task#task{tags = NewTags}),
         [remove_task_from_tag(TaskId, Tag) || Tag <- Tags],
         ok;
      _ -> {error, task_not_found}
   end.

remove_task_from_tag(TaskId, Tag) ->
   TagTasks = get_tag_tasks_by_tag(Tag),
   case TagTasks of
      [] -> ok;
      [Tsks] ->
         NewTaskList = Tsks#tag_tasks.tasks -- [TaskId],
         case NewTaskList of
            [] -> mnesia:dirty_delete(tag_tasks, Tag);
            _ -> mnesia:dirty_write(tag_tasks, #tag_tasks{tag = Tag, tasks = NewTaskList})
         end
   end.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

next_id(Table) ->
   mnesia:dirty_update_counter({ids, Table}, 1).


%%%%%%%%%%%%%%%%%%%%%%%%%%%% table managememt %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

db_init() ->
   case lists:member(node(), mnesia:table_info(schema, disc_copies)) of

      true 	-> lager:info("schema already there, loading tables from disc"),
         Tables = mnesia:system_info(tables),
         lists:foreach(fun(T) -> mnesia:force_load_table(T) end, Tables)
      ;

      false 	-> lager:info("schema must be created"),
         delete_schema(),
         case nodes() of
            [] -> create();
            [_|_] = Nodes -> add_extra_nodes(Nodes)
         end

   end.





%% deletes a local schema.
delete_schema() ->
   mnesia:stop(),
   mnesia:delete_schema([node()]),
   mnesia:start().

add_extra_nodes([Node|T]) ->
   case mnesia:change_config(extra_db_nodes, [Node]) of
      {ok, [Node]} ->
         copy_tables(node()),
         Tables = mnesia:system_info(tables),
         mnesia:wait_for_tables(Tables, ?WAIT_FOR_TABLES);
      _ ->
         add_extra_nodes(T)
   end.

copy_tables(Node) ->
   mnesia:change_table_copy_type(schema, Node, disc_copies),
   Res = mnesia:add_table_copy(schema, Node, disc_copies),
   lager:debug("remote_init schema type ~p~n", [Res]),

   Res1 = mnesia:add_table_copy(task, Node, disc_copies),
   lager:debug("remote_init add_table copy = ~p~n", [Res1]),

   Res4 = mnesia:add_table_copy(ids, Node, disc_copies),
   lager:debug("remote_init add_table copy = ~p~n", [Res4])
   ,
%%
%%   Res5 = mnesia:add_table_copy(task_tags, Node, disc_copies),
%%   lager:debug("remote_init add_table copy = ~p~n", [Res5])
%%   ,

   Res6 = mnesia:add_table_copy(tag_tasks, Node, disc_copies),
   lager:debug("remote_init add_table copy = ~p~n", [Res6])
   ,

%%   Res5 = mnesia:add_table_copy(component_state, Node, disc_copies),
%%   lager:debug("remote_init add_table copy = ~p~n", [Res5]),

   Res7 = mnesia:add_table_copy(template, Node, disc_copies),
   lager:debug("remote_init add_table copy = ~p~n", [Res7])

.

renew_tables() ->
   mnesia:delete_table(task),
   mnesia:delete_table(template),
   mnesia:delete_table(ids),
   mnesia:delete_table(tag_tasks),
   create().

create() ->
   mnesia:change_table_copy_type(schema, node(), disc_copies),

   mnesia:create_table(task, [
      {attributes, record_info(fields, task)},
      {type, set},
      {disc_copies, [node()]}, {index, [name, pid, permanent, template]}
   ]),
   mnesia:create_table(ids, [
      {attributes, record_info(fields, ids)},
      {type, set},
      {disc_copies, [node()]}
   ])
   ,
   mnesia:create_table(template, [
      {attributes, record_info(fields, template)},
      {type, set},
      {disc_copies, [node()]}, {index, [name]}
   ])
   ,
%%   Res1 = mnesia:create_table(task_tags, [
%%      {attributes, record_info(fields, task_tags)},
%%      {type, set},
%%      {disc_copies, [node()]}
%%   ])
%%   ,
   _Res2 = mnesia:create_table(tag_tasks, [
      {attributes, record_info(fields, tag_tasks)},
      {type, set},
      {disc_copies, [node()]}
   ])
%%   ,
%%   lager:notice("create tables: ~p", [Res2])

.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
export(Table) ->
   FileName = atom_to_list(node()) ++ "_export_" ++ atom_to_list(Table) ++ "_" ++
      binary_to_list(faxe_time:to_iso8601(faxe_time:now())) ++ ".txt",
   F = fun(#task{dfs = Dfs, id = Id, name = Name}, Acc) ->
      Acc ++ [{Name, Id, Dfs}]
      end,
   TFun = fun() ->  mnesia:foldl(F, [], Table) end,
   {atomic, Res} = mnesia:transaction(TFun),
   write_terms(FileName, Res).

write_terms(Filename, List) ->
   Format = fun(Term) -> io_lib:format("~tp.~n", [Term]) end,
   Text = lists:map(Format, List),
   file:write_file(Filename, Text).
