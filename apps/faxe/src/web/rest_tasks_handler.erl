%%%-------------------------------------------------------------------
%%% @author $author
%%% @copyright (C) $year, $company
%%% @doc
%%%
%%% @end
%%% Created : $fulldate
%%%-------------------------------------------------------------------
-module(rest_tasks_handler).

-include("faxe.hrl").

-define(REQ_BODY_CONSTRAINTS, #{length => 300000, period => 5000}).

%%
%% Cowboy callbacks
-export([
   init/2
   , allowed_methods/2,
   list_json/2,
   content_types_provided/2,
   update_json/2,
   start_permanent_json/2,
   stop_all_json/2,
   start_json/2,
   stop_json/2,
   start_tags_json/2,
   stop_tags_json/2,
   allow_missing_post/2,
   content_types_accepted/2,
   from_import/2,
   is_authorized/2]).

%%
%% Additional callbacks
-export([]).

-record(state, {mode}).

init(Req, [{op, Mode}]) ->
   {cowboy_rest, Req, #state{mode = Mode}}.

is_authorized(Req, State) ->
   rest_helper:is_authorized(Req, State).

allowed_methods(Req, State=#state{mode = import}) ->
   {[<<"POST">>, <<"OPTIONS">>], Req, State};
allowed_methods(Req, State) ->
    Value = [<<"GET">>, <<"OPTIONS">>],
    {Value, Req, State}.

allow_missing_post(Req, State = #state{mode = import}) ->
   {false, Req, State}.

content_types_accepted(Req = #{method := <<"POST">>}, State = #state{mode = import}) ->
   Value = [{{ <<"application">>, <<"x-www-form-urlencoded">>, []}, from_import}],
   {Value, Req, State}.


content_types_provided(Req, State=#state{mode = update}) ->
   {[
      {{<<"application">>, <<"json">>, []}, update_json}
   ], Req, State};
content_types_provided(Req, State=#state{mode = update_by_tags}) ->
   {[
      {{<<"application">>, <<"json">>, []}, update_json}
   ], Req, State};
content_types_provided(Req, State=#state{mode = update_by_template}) ->
   {[
      {{<<"application">>, <<"json">>, []}, update_json}
   ], Req, State};
content_types_provided(Req, State=#state{mode = start_permanent}) ->
   {[
      {{<<"application">>, <<"json">>, []}, start_permanent_json}
   ], Req, State};
content_types_provided(Req, State=#state{mode = start}) ->
   {[
      {{<<"application">>, <<"json">>, []}, start_json}
   ], Req, State};
content_types_provided(Req, State=#state{mode = start_by_ids}) ->
   {[
      {{<<"application">>, <<"json">>, []}, start_json}
   ], Req, State};
content_types_provided(Req, State=#state{mode = start_by_tags}) ->
   {[
      {{<<"application">>, <<"json">>, []}, start_tags_json}
   ], Req, State};
content_types_provided(Req, State=#state{mode = stop}) ->
   {[
      {{<<"application">>, <<"json">>, []}, stop_json}
   ], Req, State};
content_types_provided(Req, State=#state{mode = stop_by_ids}) ->
   {[
      {{<<"application">>, <<"json">>, []}, stop_json}
   ], Req, State};
content_types_provided(Req, State=#state{mode = stop_by_tags}) ->
   {[
      {{<<"application">>, <<"json">>, []}, stop_tags_json}
   ], Req, State};
content_types_provided(Req, State=#state{mode = stop_all}) ->
   {[
      {{<<"application">>, <<"json">>, []}, stop_all_json}
   ], Req, State};
content_types_provided(Req, State) ->
    {[
       {{<<"application">>, <<"json">>, []}, list_json}
    ], Req, State}.



from_import(Req, State=#state{}) ->
   {ok, Body, Req1} = cowboy_req:read_urlencoded_body(Req, ?REQ_BODY_CONSTRAINTS),
%%   TaskName = proplists:get_value(<<"name">>, Result),
   TasksJson = proplists:get_value(<<"tasks">>, Body),
%%   lager:notice("import JSON: ~s",[TasksJson]),
   case (catch jiffy:decode(TasksJson, [return_maps])) of
      TasksList when is_list(TasksList) -> do_import(TasksList, Req1, State);
      _ -> Req2 = cowboy_req:set_resp_body(
         jiffy:encode(
            #{<<"success">> => false, <<"message">> => <<"Error decoding json, invalid.">>}),
         Req),
         {false, Req2, State}
   end.


do_import(TasksList, Req, State) ->
   {Ok, Err} =
      lists:foldl(
         fun(TaskMap=#{<<"name">> := TName}, {OkList, ErrList}) ->
            case import_task(TaskMap) of
               {ok, _Id} -> {[TName|OkList], ErrList};
               {error, What} -> {OkList, [#{TName => faxe_util:to_bin(What)}|ErrList]}
            end
         end,
         {[],[]},
         TasksList
      ),
%%   [lager:notice("import: ~p",[import_task(T)]) || T <- TasksList],
   Req2 = cowboy_req:set_resp_body(
      jiffy:encode(
         #{total => length(TasksList), successful => length(Ok),
            errors => length(Err), messages => Err}),
      Req),
   {true, Req2, State}.


import_task(#{<<"group_leader">> := false}) -> {error, group_leader_import_only};
import_task(#{<<"dfs">> := Dfs, <<"name">> := Name, <<"permanent">> := Perm, <<"tags">> := Tags}) ->
   case faxe:register_string_task(Dfs, Name) of
      ok ->
         Id = rest_helper:get_task_or_template_id(Name, task),
         case Perm of
            true ->
               NewTask = faxe:get_task(Id),
               faxe_db:save_task(NewTask#task{permanent = true});
            false -> ok
         end,
         add_tags(Tags, Id),
         {ok, Id};
      Err -> Err
   end.

add_tags(Tags, TaskId) ->
   case Tags of
      [] -> ok;
      _ ->
         faxe:add_tags(TaskId, Tags)
   end.


list_json(Req, State=#state{mode = Mode}) ->
   #{orderby := OrderBy, dir := Direction} =
      cowboy_req:match_qs([{orderby, [], <<"changed">>}, {dir, [], <<"desc">>}], Req),
   Tasks =
      case Mode of
         list -> faxe:list_tasks();
         list_running -> faxe:list_running_tasks();
         list_by_template ->
            TId = cowboy_req:binding(template_id, Req),
            L = faxe:list_tasks_by_template(binary_to_integer(TId)),
            case L of
               {error, not_found} -> {error, template_not_found};
               _ -> L
            end;
         list_by_tags ->
            tasks_by_tags(Req);
         list_by_group ->
            GroupName = cowboy_req:binding(groupname, Req),
            L = faxe:list_tasks_by_group(GroupName),
            case L of
               {error, not_found} -> {error, group_not_found};
               _ -> L
            end
          end,
   case Tasks of
      {error, What} ->
         {jiffy:encode(#{<<"error">> => faxe_util:to_bin(What)}), Req, State};
      _ ->

         Sorted = lists:sort(order_fun(OrderBy, Direction), Tasks),
         Maps = [rest_helper:task_to_map(T) || T <- Sorted],
         {jiffy:encode(Maps), Req, State}
   end.

update_json(Req, State = #state{mode = Mode}) ->
   Result =
      case Mode of
         update -> faxe:update_all(true);
         update_by_template ->
            TemplateId = cowboy_req:binding(template, Req),
            faxe:update_by_template(TemplateId, true);
         update_by_tags ->
            Tags0 = cowboy_req:binding(tags, Req),
            Tags = binary:split(Tags0,[<<",">>, <<" ">>],[global, trim_all]),
            faxe:update_by_tags(Tags, true)
      end,
   case Result of
      R when is_list(R) ->
         Add = maybe_plural(length(R), <<"flow">>),
         rest_helper:success(Req, State, <<"updated ", Add/binary>>);
      _ ->
         rest_helper:error(Req, State)
   end.

start_permanent_json(Req, State) ->
   case faxe:start_permanent_tasks() of
      R when is_list(R) ->
         Add = maybe_plural(length(R), <<"permanent flow">>),
         rest_helper:success(Req, State, <<"started ", Add/binary>>);
      _ ->
         rest_helper:error(Req, State)
   end.

stop_all_json(Req, State) ->
   Running = faxe:list_running_tasks(),
   stop_list(Running, Req, State).

%% start a list of tasks (by string id)
start_json(Req, State) ->
   IdList = id_list(Req),
   start_list(IdList, Req, State).

start_tags_json(Req, State) ->
   Tasks = tasks_by_tags(Req),
   start_list(Tasks, Req, State).

%% stop a list of tasks (by string id)
stop_json(Req, State) ->
   IdList = id_list(Req),
   stop_list(IdList, Req, State).

stop_tags_json(Req, State) ->
   Tasks = tasks_by_tags(Req),
   stop_list(Tasks, Req, State).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
stop_list(TaskList, Req, State) ->
   StopResult =
   lists:foldl(
      fun(Task, Stopped) ->
         TId =
         case Task of
            #task{id = Id} -> Id;
            _ -> Task
         end,
         case faxe:stop_task(TId, false) of
            ok -> [TId | Stopped];
            _ -> Stopped
         end
      end, [], TaskList
   ),
   Add = maybe_plural(length(StopResult), <<"flow">>),
   rest_helper:success(Req, State, <<"stopped ", Add/binary>>).

start_list(TaskList, Req, State) ->
   StopResult =
      lists:foldl(
         fun(Task, Stopped) ->
            TId =
               case Task of
                  #task{id = Id} -> Id;
                  _ -> Task
               end,
            case faxe:start_task(TId) of
               {ok, _} -> [TId | Stopped];
               _ -> Stopped
            end
         end, [], TaskList
      ),
   Add = maybe_plural(length(StopResult), <<"flow">>),
   rest_helper:success(Req, State, <<"started ", Add/binary>>).

id_list(Req) ->
   Ids = cowboy_req:binding(ids, Req),
   L = binary:split(Ids,[<<",">>, <<" ">>],[global, trim_all]),
   [rest_helper:int_or_bin(E) || E <- L].

tasks_by_tags(Req) ->
   Tags = cowboy_req:binding(tags, Req),
   TagList = binary:split(Tags,[<<",">>, <<" ">>],[global, trim_all]),
   faxe:list_tasks_by_tags(TagList).

order_fun(<<"id">>, Dir) ->
   fun(#task{id = AId}, #task{id = BId}) -> (AId =< BId) == order_dir(Dir) end;
order_fun(<<"name">>, Dir) ->
   fun(#task{name = AId}, #task{name = BId}) -> (AId =< BId) == order_dir(Dir) end;
order_fun(<<"last_start">>, Dir) ->
   fun(#task{last_start = AId}, #task{last_start = BId}) -> (AId =< BId) == order_dir(Dir) end;
order_fun(<<"last_stop">>, Dir) ->
   fun(#task{last_stop = AId}, #task{last_stop = BId}) -> (AId =< BId) == order_dir(Dir) end;
order_fun(_, Dir) ->
   fun(#task{date = AId}, #task{date = BId}) -> (AId =< BId) == order_dir(Dir) end.

order_dir(<<"asc">>) -> true;
order_dir(_) -> false.


maybe_plural(Num, Word) when is_integer(Num), is_binary(Word) ->
   NumBin = integer_to_binary(Num),
   NewWord =
   case Num of
      1 -> Word;
      _ -> <<Word/binary, "s">>
   end,
   <<NumBin/binary, " ", NewWord/binary>>.
