%% Date: 28.04.17 - 20:09
%% Ⓒ 2017 heyoka
-module(faxe_db).
-author("Alexander Minichmair").
-include("faxe.hrl").


%% API

%% db management
-export([renew_tables/0, create/0]).

%% api export
-export([get_task/1, save_task/1, delete_task/1, get_all_tasks/0]).

get_all_tasks() ->
   [mnesia:dirty_read(task, Key) || Key <- mnesia:dirty_all_keys(task)].

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

next_id(Table) ->
   mnesia:dirty_update_counter({ids, Table}, 1).


%% table managememt

renew_tables() ->
   mnesia:delete_table(task),
   mnesia:delete_table(ids),
   create().

create() ->
   mnesia:change_table_copy_type(schema, node(), disc_copies),

   mnesia:create_table(task, [
      {attributes, record_info(fields, task)},
      {type, set},
      {disc_copies, [node()]}, {index, [name]}
   ]),
   mnesia:create_table(ids, [
      {attributes, record_info(fields, ids)},
      {type, set},
      {disc_copies, [node()]}
   ])
%%   ,
%%   mnesia:create_table(template, [
%%      {attributes, record_info(fields, template)},
%%      {type, set},
%%      {disc_copies, [node()]}
%%   ])
.