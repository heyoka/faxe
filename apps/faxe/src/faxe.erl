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
   start_permanent_tasks/0,
   get_stats/1,
   update_file_task/2,
   update_string_task/2,
   update_task/3,
   update/3,
   get_errors/1,
   list_permanent_tasks/0,
   get_task/1,
   ping_task/1,
   start_temp/2,
   start_file_temp/2,
   export/1,
   get_template/1,
   list_temporary_tasks/0,
   list_tasks_by_template/1,
   list_tasks_by_tags/1,
   get_all_tags/0,
   add_tags/2,
   remove_tags/2,
   get_logs/4,
   set_tags/2,
   get_graph/1,
   task_to_graph/1,
   start_trace/1,
   stop_trace/1,
   update_all/0,
   stop_task_group/2, delete_task_group/1, list_tasks_by_group/1, set_group_size/2, update_by_tags/1, update_by_template/1]).

start_permanent_tasks() ->
   Tasks = faxe_db:get_permanent_tasks(),
   [start_task(T#task.id, true) || T <- Tasks].

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


-spec get_task(term()) -> {error, not_found}|#task{}.
get_task(TaskId) ->
   case faxe_db:get_task(TaskId) of
      {error, not_found} -> {error, not_found};
      #task{pid = Pid} = T ->
         Running = supervisor:which_children(graph_sup),
         case lists:keyfind(Pid, 2, Running) of
            Y when is_tuple(Y) -> T#task{is_running = true};
            false -> T
         end
   end.

%% @doc get the graph definition (nodes and edges) as a map
-spec get_graph(non_neg_integer()|binary()) -> map() | {error, term()}.
get_graph(TaskId) ->
   case get_task(TaskId) of
      #task{} = T ->
         task_to_graph(T);
      O -> O
   end.


task_to_graph(#task{definition = #{edges := Edges, nodes := Nodes} } = _T) ->
   EdgesOut = [
      #{<<"src">> => Source, <<"src_port">> => PortOut, <<"dest">> => Dest, <<"dest_port">> => PortIn}
      || {Source, PortOut, Dest, PortIn, _} <- Edges
   ],
   NodesOut = [
      #{<<"name">> => NName, <<"type">> => NType} || {NName, NType, _Opts} <- Nodes
   ],
   #{nodes => NodesOut, edges => EdgesOut}.

-spec get_template(term()) -> {error, not_found}|#template{}.
get_template(TemplateId) ->
   case faxe_db:get_template(TemplateId) of
      {error, not_found} -> {error, not_found};
      #template{} = T -> T
   end.

get_all_tags() ->
   faxe_db:get_all_tags().

-spec list_tasks() -> list().
list_tasks() ->
   add_running_flag(faxe_db:get_all_tasks()).

-spec list_templates() -> list().
list_templates() ->
   faxe_db:get_all_templates().

-spec list_running_tasks() -> list(#task{}).
list_running_tasks() ->
   Graphs = supervisor:which_children(graph_sup),
   [T#task{is_running = true} || T <-  faxe_db:get_tasks_by_pids(Graphs)].

list_permanent_tasks() ->
   faxe_db:get_permanent_tasks().

list_temporary_tasks() ->
   Tasks = ets:tab2list(temp_tasks),
   Tasks.

list_tasks_by_template(TemplateId) when is_integer(TemplateId) ->
   case get_template(TemplateId) of
      #template{name = Name} -> list_tasks_by_template(Name);
      Other -> Other
   end;
list_tasks_by_template(TemplateName) when is_binary(TemplateName) ->
   add_running_flag(faxe_db:get_tasks_by_template(TemplateName)).

list_tasks_by_tags(TagList) when is_list(TagList) ->
   add_running_flag(faxe_db:get_tasks_by_tags(TagList)).

list_tasks_by_group(GroupName) when is_binary(GroupName) ->
   add_running_flag(faxe_db:get_tasks_by_group(GroupName)).

add_tags(TaskId, Tags) ->
   faxe_db:add_tags(TaskId, Tags).

remove_tags(TaskId, Tags) ->
   faxe_db:remove_tags(TaskId, Tags).

set_tags(TaskId, Tags) ->
   faxe_db:set_tags(TaskId, Tags).

add_running_flag(TaskList) when is_list(TaskList) ->
   Running = supervisor:which_children(graph_sup),
   F =
      fun(#task{pid = Pid} = T) ->
         case lists:keyfind(Pid, 2, Running) of
            Y when is_tuple(Y) -> T#task{is_running = true};
            false -> T#task{is_running = false}
         end
      end,
   lists:map(F, TaskList);
add_running_flag(What) ->
   What.

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
         case eval_dfs(DfsScript, Type) of
            {DFS, Def} when is_map(Def) ->
               Task = #task{
                  date = faxe_time:now_date(),
                  dfs = DFS,
                  definition = Def,
                  name = Name,
                  group = Name,
                  group_leader = true
               },
               faxe_db:save_task(Task);

            {error, What} -> {error, What}
         end;
      _T ->
         {error, task_exists}
   end.

%% @doc update all tasks that exist, use with care
-spec update_all() -> [ok|{error, term()}].
update_all() ->
   update_list(list_tasks()).

update_by_tags(Tags) when is_list(Tags) ->
   Tasks = list_tasks_by_tags(Tags),
   update_list(Tasks).

update_by_template(TemplateId) ->
   Tasks = list_tasks_by_template(TemplateId),
   update_list(Tasks).

update_list(TaskList) when is_list(TaskList) ->
   [update_task(DfsScript, Id, data) || #task{id = Id, dfs = DfsScript} <- TaskList].

%%set_all_offline() ->
%%   [
%%      begin
%%         [T] = mnesia:dirty_read(task, Id),
%%         mnesia:dirty_write(T#task{is_running = false})
%%      end
%%      || Id <- mnesia:dirty_all_keys(task)
%%   ].

-spec update_file_task(list(), integer()|binary()) -> ok|{error, term()}.
update_file_task(DfsFile, TaskId) ->
   update_task(DfsFile, TaskId, file).

-spec update_string_task(binary(), integer()|binary()) -> ok|{error, term()}.
update_string_task(DfsScript, TaskId) ->
   update_task(DfsScript, TaskId, data).

-spec update_task(list()|binary(), integer()|binary(), atom()) -> ok|{error, term()}.
update_task(DfsScript, TaskId, ScriptType) ->
   Res =
   case get_running(TaskId) of
      {true, T=#task{}} -> {update_running(DfsScript, T, ScriptType), T};
      {false, T=#task{}} -> {update(DfsScript, T, ScriptType), T};
%%      {_, T=#task{group_leader = false}} -> {error, group_leader_update_only};
      Err -> {Err, nil}
   end,
   case Res of
      {ok, _Task = #task{group_leader = true, group = Group}} ->
         GroupMembers = faxe_db:get_tasks_by_group(Group),
         case GroupMembers of
            {error, not_found} -> ok;
            L when is_list(L) ->
               %% update group-members
               [update_task(DfsScript, Id, ScriptType)
                  || #task{id = Id, group_leader = Lead} <- L, Lead == false],
               ok
         end;
      {ok, _Task} -> ok;
      {Error, nil} -> Error
   end.


-spec update(list()|binary(), #task{}, atom()) -> ok|{error, term()}.
update(DfsScript, Task, ScriptType) ->
   case eval_dfs(DfsScript, ScriptType) of
      {DFS, Map} when is_map(Map) ->
         NewTask = Task#task{
            definition = Map,
            dfs = DFS,
            date = faxe_time:now_date()},
         faxe_db:save_task(NewTask);
      Err -> Err
   end.

-spec update_running(list()|binary(), #task{}, atom()) -> ok|{error, term()}.
update_running(DfsScript, Task = #task{id = TId, pid = TPid}, ScriptType) ->
   erlang:monitor(process, TPid),
   stop_task(Task, false),
   case update(DfsScript, Task, ScriptType) of
      {error, Err} -> {error, Err};
      ok ->
         receive
            {'DOWN', _MonitorRef, process, TPid, _Info} ->
               start_task(TId, Task#task.permanent), ok
            after 5000 ->
               catch erlang:demonitor(TPid, true), {error, updated_task_start_timeout}
         end
   end.

-spec eval_dfs(list()|binary(), file|data) ->
   {DFSString :: list(), GraphDefinition :: map()} | {error, term()}.
eval_dfs(DfsScript, Type) ->
   try faxe_dfs:Type(DfsScript, []) of
      {_DFSString, {error, What}} -> {error, What};
      {error, What} -> {error, What};
      {_DFSString, Def} = Result when is_map(Def) -> Result;
      E -> E
   catch
%%      throw:Err:Stacktrace -> {error, Err};
%%      exit:Err:Stacktrace -> {error, Err};
%%      error:Err:Stacktrace -> {error, Err};
      _:Err:Stacktrace      ->
         lager:warning("stacktrace: ~p",[Stacktrace]),
         {error, Err}
   end.

%% @doc get a task by its id and also if it is currently running
-spec get_running(integer()|binary()) -> {error, term()}|{true|false, #task{}}.
get_running(TaskId) ->
   case faxe_db:get_task(TaskId) of
      {error, Error} -> {error, Error};
      T = #task{} ->
         case is_task_alive(T) of
            true -> {true, T};
            false -> {false, T}
         end
   end.

%% get the dfs binary
get_file_dfs(DfsFile) ->
   {ok, DfsParams} = application:get_env(faxe, dfs),
   Path = proplists:get_value(script_path, DfsParams),
   {ok, Data} = file:read_file(Path++DfsFile),
   binary:replace(Data, <<"\\">>, <<>>, [global]).

-spec register_template_file(list(), binary()) ->
   'ok'|{error, template_exists}|{error, not_found}|{error, Reason::term()}.
register_template_file(DfsFile, TemplateName) ->
   StringData = binary_to_list(get_file_dfs(DfsFile)),
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

         case eval_dfs(DfsScript, Type) of
            {DFS, Def} when is_map(Def) ->
               Template = #template{
                  date = faxe_time:now_date(),
                  definition = Def,
                  name = Name,
                  dfs = DFS
               },
               faxe_db:save_template(Template);

            {error, What} -> {error, What}
         end;

      _T ->
         {error, template_exists}
   end.

task_from_template(TemplateId, TaskName) ->
   task_from_template(TemplateId, TaskName, #{}).
task_from_template(TemplateId, TaskName, Vars) when is_map(Vars) ->
   case faxe_db:get_task(TaskName) of
      {error, not_found} -> case faxe_db:get_template(TemplateId) of
                               {error, not_found} -> {error, template_not_found};
                               Template = #template{} -> template_to_task(Template, TaskName, Vars), ok
                            end;
      #task{} -> {error, task_exists}
   end.


start_file_temp(DfsScript, TTL) ->
   start_temp(DfsScript, file, TTL).

start_temp(DfsScript, TTL) ->
   start_temp(DfsScript, data, TTL).
start_temp(DfsScript, Type, TTL) ->
   case eval_dfs(DfsScript, Type) of
      {DFS, Def} when is_map(Def) ->
         Id = list_to_binary(faxe_util:uuid_string()),
         case dataflow:create_graph(Id, Def) of
            {ok, Graph} ->
               _Task = #task{
                  date = faxe_time:now_date(),
                  dfs = DFS,
                  definition = Def,
                  name = Id
               },
               ets:insert(temp_tasks, {Id, Graph}),
               try dataflow:start_graph(Graph, #task_modes{temporary = true, temp_ttl = TTL}) of
                  ok ->
                     {ok, Id}
               catch
                  _:E = E -> {error, graph_start_error}
               end;
            {error, E} -> {error, E}
         end;
      {error, What} -> {error, What}
   end.

%%%------------------------------------------------
%%% start task
%%%------------------------------------------------
start_task(TaskId) ->
   start_task(TaskId, false).
start_task(TaskId, #task_modes{run_mode = _RunMode} = Mode) ->
   case faxe_db:get_task(TaskId) of
      {error, not_found} -> {error, task_not_found};
      T = #task{} -> do_start_task(T, Mode)
   end;
start_task(TaskId, Permanent) when Permanent == true orelse Permanent == false ->
   start_task(TaskId, push, Permanent).
-spec start_task(integer()|binary(), atom(), true|false) -> ok|{error, term()}.
start_task(TaskId, GraphRunMode, Permanent) ->
   start_task(TaskId, #task_modes{run_mode = GraphRunMode, permanent = Permanent}).

do_start_task(T = #task{name = Name, definition = GraphDef},
    #task_modes{concurrency = Concurrency, permanent = Perm} = Mode) ->
   case dataflow:create_graph(Name, GraphDef) of
      {ok, Graph} ->
         try dataflow:start_graph(Graph, Mode) of
            ok ->
               faxe_db:save_task(T#task{pid = Graph, last_start = faxe_time:now_date(), permanent = Perm}),
               case Concurrency of
                  1 -> {ok, Graph};
                  Num when Num > 1 ->
                     start_concurrent(T, Mode),
                     {ok, Graph}
               end
         catch
            _:E = E -> {error, graph_start_error}
         end;
      {error, {already_started, _Pid}} -> {error, already_started}
   end.

start_concurrent(Task = #task{}, #task_modes{concurrency = Con} = Mode) ->
   F = fun(Num) -> start_copy(Task, Mode, Num) end,
   lists:map(F, lists:seq(2, Con)).

start_copy(Task = #task{definition = GraphDef, name = TName}, #task_modes{permanent = Perm} = Mode, Num) ->
   NumBin = integer_to_binary(Num),
   Name = <<TName/binary, "--", NumBin/binary>>,
   case faxe_db:get_task(Name) of
      {error, not_found} ->
         case dataflow:create_graph(Name, GraphDef) of
            {ok, Graph} ->
               try dataflow:start_graph(Graph, Mode) of
                  ok ->
                     faxe_db:save_task(
                        Task#task{
                           pid = Graph, name = Name,
                           id = undefined, group_leader = false,
                           last_start = faxe_time:now_date(),
                           permanent = Perm, group = TName})
               catch
                  _:E = E -> {error, graph_start_error}
               end;
            {error, What} -> {error, What}
         end;
      T = #task{} -> do_start_task(T#task{group_leader = false, group = TName}, Mode#task_modes{concurrency = 1})
   end
   .

%%%-------------------------------------------------
%%% task group
%%%-------------------------------------------------
-spec set_group_size(binary(), non_neg_integer()) -> ok|list().
set_group_size(GroupName, NewSize) when is_binary(GroupName), is_integer(NewSize) ->
   case faxe_db:get_tasks_by_group(GroupName) of
      {error, not_found} -> {error, group_not_found};
      GroupList when is_list(GroupList) ->
         Leader = get_group_leader(GroupList),
         case is_task_alive(Leader) of
            false -> {error, not_running};
            true ->
               RunningMembers = [T || T <- GroupList, is_task_alive(T)],
               case NewSize - length(RunningMembers) of
                  N when N >= 0 -> %% we want more
                     start_concurrent(Leader,
                        #task_modes{concurrency = NewSize, permanent = Leader#task.permanent});
                  N1 when N1 < 0 -> %% we want less
                     Num = abs(N1),
                     SB = byte_size(GroupName),
                     SortFun =
                     fun
                        (#task{name = <<GN:SB/binary, "--", Rank/binary>>},
                            #task{name = <<GN:SB/binary, "--", OtherRank/binary>>}) ->
                           binary_to_integer(Rank) > binary_to_integer(OtherRank);
                        (_, _) -> true
                     end,
                     Sorted = lists:sort(SortFun, RunningMembers),
                     del_group_members(Sorted, Num)
               end
         end
   end.


del_group_members([], _) ->
   [];
del_group_members(GroupList, 0) ->
   GroupList;
del_group_members([#task{group_leader = true} | R], Num) ->
   del_group_members(R, Num);
del_group_members([T=#task{group_leader = false, id = TaskId} | R], Num) ->
   do_stop_task(T, false),
   faxe_db:delete_task(TaskId),
   del_group_members(R, Num - 1).



get_group_leader([]) ->
   {error, group_leader_not_found};
get_group_leader([#task{group_leader = false} | R]) ->
   get_group_leader(R);
get_group_leader([T=#task{group_leader = true} | _R]) ->
   T.

-spec stop_task(integer()|binary()|#task{}) -> ok.
%% @doc just stop the graph process and its children
stop_task(_T=#task{pid = Graph}) when is_pid(Graph) ->
   case is_process_alive(Graph) of
      true ->
         df_graph:stop(Graph);
      false -> {error, not_running}
   end;

stop_task(TaskId) ->
   stop_task(TaskId, false).
-spec stop_task(#task{}|integer()|binary(), true|false) -> ok.
stop_task(T = #task{}, Permanent) ->
   do_stop_task(T, Permanent);
stop_task(TaskId, Permanent) ->
   T = faxe_db:get_task(TaskId),
   case T of
      {error, not_found} -> {error, not_found};
      #task{pid = Graph} = T when is_pid(Graph) -> do_stop_task(T, Permanent);
      #task{} -> {error, not_running}
   end.

%% stop all tasks running with a specific group name
stop_task_group(TaskGroupName, Permanent) ->
   Tasks = faxe_db:get_tasks_by_group(TaskGroupName),
   case Tasks of
      {error, not_found} -> {error, not_found};
      TaskList when is_list(TaskList) ->
         [do_stop_task(T, Permanent) || T <- TaskList]
   end.

do_stop_task(T = #task{pid = Graph, group_leader = _Leader, group = _Group}, Permanent) ->
   case is_task_alive(T) of
      true ->
         df_graph:stop(Graph),
         NewT =
            case Permanent of
               true -> T#task{permanent = false};
               false -> T
            end,
         faxe_db:save_task(NewT#task{pid = undefined, last_stop = faxe_time:now_date()});
%%         case Leader of
%%            true ->
%%               GroupMembers = faxe_db:get_tasks_by_group(Group),
%%               case GroupMembers of
%%                  {error, not_found} -> ok;
%%                  L when is_list(L) ->
%%                     %% stop group-members
%%                     [do_stop_task(Task, Permanent) || Task <- L], ok
%%               end;
%%            false -> ok
%%         end;
      false -> {error, not_running}
   end.

-spec delete_task(term()) -> ok | {error, not_found} | {error, task_is_running}.
delete_task(TaskId) ->
   T = faxe_db:get_task(TaskId),
   case T of
      {error, not_found} -> {error, not_found};
      T = #task{} -> do_delete_task(T)
   end.

do_delete_task(T = #task{id = TaskId, group = Group, group_leader = Leader}) ->
   case is_task_alive(T) of
      true -> {error, task_is_running};
      false ->
         case faxe_db:delete_task(TaskId) of
            ok -> case Leader of true -> delete_task_group(Group), ok; false -> ok end;
            Else -> Else
         end
   end.

%% delete all tasks from db with group == TaskGroupName
delete_task_group(TaskGroupName) ->
   Tasks = faxe_db:get_tasks_by_group(TaskGroupName),
   case Tasks of
      {error, not_found} -> {error, not_found};
      TaskList when is_list(TaskList) ->
         case lists:all(fun(#task{} = T) -> is_task_alive(T) == false end, TaskList) of
            true -> [faxe_db:delete_task(T) || T <- TaskList];
            false -> {error, tasks_are_running}
         end
   end.

delete_template(TaskId) ->
   T = faxe_db:get_template(TaskId),
   case T of
      {error, not_found} -> ok;
      #template{} ->
         faxe_db:delete_template(TaskId)
   end.

is_task_alive(#task{pid = Graph}) when is_pid(Graph) ->
   is_process_alive(Graph) =:= true;
is_task_alive(_) -> false.

-spec ping_task(term()) -> {ok, NewTimeout::non_neg_integer()} | {error, term()}.
ping_task(TaskId) ->
   case ets:lookup(temp_tasks, TaskId) of
      [] -> {error, not_found};
      [{TaskId, GraphPid}] -> df_graph:ping(GraphPid)
   end.

%% @deprecated
get_stats(TaskId) ->
   T = faxe_db:get_task(TaskId),
   case T of
      {error, not_found} -> {error, not_found};
      #task{pid = Graph} when is_pid(Graph) ->
         case is_process_alive(Graph) of
            true -> df_graph:get_stats(Graph);
            false -> {error, task_not_running}
         end;
      #task{} -> {ok, []}
   end.

%% @deprecated
-spec get_errors(integer()|binary()) -> {error, term()} | {ok, term()}.
get_errors(TaskId) ->
   T = faxe_db:get_task(TaskId),
   case T of
      {error, not_found} -> {error, not_found};
      #task{pid = Graph} when is_pid(Graph) ->
         case is_process_alive(Graph) of
            true -> df_graph:get_errors(Graph);
            false -> {error, task_not_running}
         end;
      #task{} -> {ok, []}
   end.

%% @deprecated
-spec get_logs(integer()|binary(), binary(), non_neg_integer(), non_neg_integer()) ->
   {error, term()} | {ok, list(map())}.
get_logs(TaskId, Severity, MaxAge, Limit) ->
   T = faxe_db:get_task(TaskId),
   case T of
      {error, not_found} -> {error, not_found};
      #task{name = Name} -> crate_log_reader:read_logs(Name, Severity, MaxAge, Limit)
   end.

export(TaskId) ->
   T = faxe_db:get_task(TaskId),
   case T of
      {error, not_found} -> {error, not_found};
      #task{pid = Graph} when is_pid(Graph) ->
         case is_process_alive(Graph) of
            true -> df_graph:export(Graph);
            false -> {error, task_not_running}
         end;
      #task{} -> {ok, []}
   end.

-spec start_trace(non_neg_integer()|binary()) -> {ok, pid()} | {error, not_found} | {error_task_not_running}.
start_trace(TaskId) ->
   T = faxe_db:get_task(TaskId),
   case T of
      {error, not_found} -> {error, not_found};
      #task{pid = Graph} when is_pid(Graph) ->
         case is_process_alive(Graph) of
            true ->
               df_graph:start_trace(Graph),
               {ok, Graph};
            false -> {error, task_not_running}
         end;
      #task{} -> {error, task_not_running}
   end.

-spec stop_trace(non_neg_integer()|binary()) -> {ok, pid()} | {error, not_found} | {error_task_not_running}.
stop_trace(TaskId) ->
   T = faxe_db:get_task(TaskId),
   case T of
      {error, not_found} -> {error, not_found};
      #task{pid = Graph} when is_pid(Graph) ->
         case is_process_alive(Graph) of
            true -> df_graph:stop_trace(Graph), {ok, Graph};
            false -> {error, task_not_running}
         end;
      #task{} -> {error, task_not_running}
   end.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec template_to_task(#template{}, binary(), map()) -> ok | {error, term()}.
template_to_task(Template = #template{dfs = DFS}, TaskName, Vars) ->
   {DFSString, Def} = faxe_dfs:data(DFS, Vars),
   case Def of
      _ when is_map(Def) ->
         Task = #task{
            date = faxe_time:now_date(),
            definition = Def,
            dfs = DFSString,
            name = TaskName,
            template = Template#template.name,
            template_vars = Vars
            },
         faxe_db:save_task(Task);
      {error, What} -> {error, What}
   end.