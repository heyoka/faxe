%% Date: 28.04.17 - 23:17
%% Ⓒ 2017 Alexander Minichmair
-module(faxe).
-author("Alexander Minichmair").

-include("faxe.hrl").
%% API
-export([
   register_task/3,
   register_file/2,
   register_string/2,
   list_tasks/0,
   start_task/1,
   stop_task/1,
   delete_task/1
]).


-spec list_tasks() -> list().
list_tasks() ->
   faxe_db:get_all_tasks().

-spec register_file(list()|binary(), any()) -> any().
register_file(DfsScript, Name) ->
   register_task(DfsScript, Name, file).

-spec register_string(list()|binary(), any()) -> any().
register_string(DfsScript, Name) ->
   register_task(DfsScript, Name, data).


register_task(DfsScript, Name, Type) when is_list(DfsScript) ->
   case faxe_db:get_task(Name) of
      {error, not_found} ->
         Task = faxe_dfs:Type(DfsScript, Name),
         case Task of
            T when is_tuple(T) -> faxe_db:save_task(Task);
            {error, What} -> {error, What}
         end;
      _T ->
         {error, task_exists}
   end.


start_task(TaskId) ->
   case faxe_db:get_task(TaskId) of
      {error, not_found} -> {error, task_not_found};
      T = #task{definition = GraphDef, name = Name} ->
         {ok, Graph} = dataflow:create_graph(Name, GraphDef),
         ok = dataflow:start_graph(Graph, push),
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
