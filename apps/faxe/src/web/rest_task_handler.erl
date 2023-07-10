%%%-------------------------------------------------------------------
%%% @author $author
%%% @copyright (C) $year, $company
%%% @doc
%%%
%%% @end
%%% Created : $fulldate
%%%-------------------------------------------------------------------
-module(rest_task_handler).

-define(BODY_LENGTH_TIMEOUT, #{length => 500000, period => 7000}).

%%
%% Cowboy callbacks
-export([
   init/2,
   allowed_methods/2,
   content_types_provided/2,
   resource_exists/2,
   content_types_accepted/2,
   delete_resource/2
   , allow_missing_post/2,
   malformed_request/2,
   is_authorized/2]).

%%
%% Additional callbacks
-export([
   from_register_task/2,
   get_to_json/2,
   get_graph_to_json/2,
   from_update_to_json/2,
   create_to_json/2,
   start_to_json/2,
   stop_to_json/2,
   from_ping_to_json/2,
   from_start_temp_task/2,
   remove_tags_from_json/2,
   add_tags_from_json/2,
   start_debug_to_json/2,
   stop_debug_to_json/2,
   start_group_to_json/2,
   stop_group_to_json/2,
   set_group_size_to_json/2, from_upsert_task/2, start_metrics_trace_to_json/2, stop_metrics_trace_to_json/2]).

-include("faxe.hrl").

-record(state, {mode, task_id, task, tags, name, dfs, delete_force = false}).

init(Req, [{op, Mode}]) ->
   TId = cowboy_req:binding(task_id, Req),
   TaskId = rest_helper:int_or_bin(TId),
   {cowboy_rest, Req, #state{mode = Mode, task_id = TaskId}}.

is_authorized(Req, State) ->
   rest_helper:is_authorized(Req, State).

allowed_methods(Req, State=#state{mode = get}) ->
   {[<<"GET">>], Req, State};
allowed_methods(Req, State=#state{mode = get_graph}) ->
   {[<<"GET">>], Req, State};
allowed_methods(Req, State=#state{mode = upsert}) ->
   {[<<"POST">>, <<"PUT">>], Req, State};
allowed_methods(Req, State=#state{mode = register}) ->
   {[<<"POST">>], Req, State};
allowed_methods(Req, State=#state{mode = start_temp}) ->
   {[<<"POST">>], Req, State};
allowed_methods(Req, State=#state{mode = start_debug}) ->
   {[<<"GET">>], Req, State};
allowed_methods(Req, State=#state{mode = stop_debug}) ->
   {[<<"GET">>], Req, State};
allowed_methods(Req, State=#state{mode = start_metrics_trace}) ->
   {[<<"GET">>], Req, State};
allowed_methods(Req, State=#state{mode = stop_metrics_trace}) ->
   {[<<"GET">>], Req, State};
allowed_methods(Req, State=#state{mode = update}) ->
   {[<<"POST">>], Req, State};
allowed_methods(Req, State=#state{mode = ping}) ->
   {[<<"POST">>], Req, State};
allowed_methods(Req, State=#state{mode = start}) ->
   {[<<"GET">>], Req, State};
allowed_methods(Req, State=#state{mode = start_group}) ->
   {[<<"GET">>], Req, State};
allowed_methods(Req, State=#state{mode = stop}) ->
   {[<<"GET">>], Req, State};
allowed_methods(Req, State=#state{mode = stop_group}) ->
   {[<<"GET">>], Req, State};
allowed_methods(Req, State=#state{mode = delete_force}) ->
   {[<<"DELETE">>], Req, State#state{mode = delete, delete_force = true}};
allowed_methods(Req, State=#state{mode = delete}) ->
   {[<<"DELETE">>], Req, State};
allowed_methods(Req, State=#state{mode = delete_group}) ->
   {[<<"DELETE">>], Req, State};
allowed_methods(Req, State=#state{mode = remove_tags}) ->
   {[<<"POST">>], Req, State};
allowed_methods(Req, State=#state{mode = add_tags}) ->
   {[<<"POST">>], Req, State};
allowed_methods(Req, State=#state{mode = set_group_size}) ->
   {[<<"GET">>], Req, State};
allowed_methods(Req, State=#state{}) ->
   {[], Req, State}.

allow_missing_post(Req, State = #state{mode = Mode}) when Mode == remove_tags; Mode == add_tags ->
   {false, Req, State};
allow_missing_post(Req, State) ->
   {true, Req, State}.

content_types_accepted(Req, State = #state{mode = upsert}) ->
   Value = [{{ <<"application">>, <<"x-www-form-urlencoded">>, []}, from_upsert_task}],
   {Value, Req, State};
content_types_accepted(Req = #{method := <<"POST">>}, State = #state{mode = register}) ->
    Value = [{{ <<"application">>, <<"x-www-form-urlencoded">>, []}, from_register_task}],
    {Value, Req, State};
content_types_accepted(Req = #{method := <<"POST">>}, State = #state{mode = start_temp}) ->
   Value = [{{ <<"application">>, <<"x-www-form-urlencoded">>, []}, from_start_temp_task}],
   {Value, Req, State};
content_types_accepted(Req = #{method := <<"POST">>} , State = #state{mode = update}) ->
   Value = [{{ <<"application">>, <<"x-www-form-urlencoded">>, []}, from_update_to_json}],
   {Value, Req, State};
content_types_accepted(Req = #{method := <<"POST">>} , State = #state{mode = ping}) ->
   Value = [{{ <<"application">>, <<"x-www-form-urlencoded">>, []}, from_ping_to_json}],
   {Value, Req, State};
content_types_accepted(Req = #{method := <<"POST">>} , State = #state{mode = remove_tags}) ->
   Value = [{{ <<"application">>, <<"x-www-form-urlencoded">>, []}, remove_tags_from_json}],
   {Value, Req, State};
content_types_accepted(Req = #{method := <<"POST">>} , State = #state{mode = add_tags}) ->
   Value = [{{ <<"application">>, <<"x-www-form-urlencoded">>, []}, add_tags_from_json}],
   {Value, Req, State}.


content_types_provided(Req, State=#state{mode = get}) ->
    {[
       {{<<"application">>, <<"json">>, []}, get_to_json},
       {{<<"text">>, <<"html">>, []}, get_to_json}
    ], Req, State};
content_types_provided(Req, State=#state{mode = get_graph}) ->
   {[
      {{<<"application">>, <<"json">>, []}, get_graph_to_json},
      {{<<"text">>, <<"html">>, []}, get_graph_to_json}
   ], Req, State};
content_types_provided(Req, State=#state{mode = start}) ->
   {[
      {{<<"application">>, <<"json">>, []}, start_to_json},
      {{<<"text">>, <<"html">>, []}, start_to_json}
   ], Req, State};
content_types_provided(Req, State=#state{mode = start_group}) ->
   {[
      {{<<"application">>, <<"json">>, []}, start_group_to_json}
   ], Req, State};
content_types_provided(Req, State=#state{mode = start_debug}) ->
   {[
      {{<<"application">>, <<"json">>, []}, start_debug_to_json},
      {{<<"text">>, <<"html">>, []}, start_debug_to_json}
   ], Req, State};
content_types_provided(Req, State=#state{mode = stop_debug}) ->
   {[
      {{<<"application">>, <<"json">>, []}, stop_debug_to_json},
      {{<<"text">>, <<"html">>, []}, stop_debug_to_json}
   ], Req, State};
content_types_provided(Req, State=#state{mode = start_metrics_trace}) ->
   {[
      {{<<"application">>, <<"json">>, []}, start_metrics_trace_to_json}
   ], Req, State};
content_types_provided(Req, State=#state{mode = stop_metrics_trace}) ->
   {[
      {{<<"application">>, <<"json">>, []}, stop_metrics_trace_to_json}
   ], Req, State};
content_types_provided(Req, State=#state{mode = stop}) ->
   {[
      {{<<"application">>, <<"json">>, []}, stop_to_json},
      {{<<"text">>, <<"html">>, []}, stop_to_json}
   ], Req, State};
%%content_types_provided(Req, State=#state{mode = delete}) ->
%%   {[
%%      {{<<"application">>, <<"json">>, []}, do_delete}
%%   ], Req, State};
content_types_provided(Req, State=#state{mode = stop_group}) ->
   {[
      {{<<"application">>, <<"json">>, []}, stop_group_to_json}
   ], Req, State};
content_types_provided(Req, State=#state{mode = set_group_size}) ->
   {[
      {{<<"application">>, <<"json">>, []}, set_group_size_to_json}
   ], Req, State};
content_types_provided(Req0 = #{method := _Method}, State=#state{mode = _Mode}) ->
   {[
      {{<<"application">>, <<"json">>, []}, create_to_json},
      {{<<"text">>, <<"html">>, []}, create_to_json}
   ], Req0, State}.
%%.

malformed_request(Req, State=#state{mode = Mode}) when Mode == add_tags; Mode == remove_tags ->
   {ok, Result, Req1} = cowboy_req:read_urlencoded_body(Req),
   TagsJson = proplists:get_value(<<"tags">>, Result, undefined),
   TagList = convert_tags(TagsJson),
   Malformed = TagList == invalid,
   {Malformed,
      rest_helper:report_malformed(Malformed, Req1, [<<"tags">>]), State#state{tags = TagList}};
malformed_request(Req, State=#state{mode = Mode}) when Mode == register orelse Mode == upsert ->
   L = cowboy_req:body_length(Req),
   {ok, Result, Req1} = cowboy_req:read_urlencoded_body(Req, ?BODY_LENGTH_TIMEOUT),
   Dfs = proplists:get_value(<<"dfs">>, Result, invalid),
   Name = proplists:get_value(<<"name">>, Result, invalid),
   Tags = convert_tags(proplists:get_value(<<"tags">>, Result, undefined)),
   {_Good, MalformedP} = lists:splitwith(fun({_N, E}) -> E /= invalid end,
      [{<<"dfs">>, Dfs}, {<<"name">>, Name}, {<<"tags">>, Tags}]),
   Malformed = MalformedP /= [],
   {Malformed, rest_helper:report_malformed(Malformed, Req1, proplists:get_keys(MalformedP)),
      State#state{dfs = Dfs, name = Name, tags = Tags}};
malformed_request(Req, State=#state{mode = Mode}) when Mode == update ->
   {ok, Result, Req1} = cowboy_req:read_urlencoded_body(Req, ?BODY_LENGTH_TIMEOUT),
   Dfs = proplists:get_value(<<"dfs">>, Result, invalid),
   Tags = convert_tags(proplists:get_value(<<"tags">>, Result, undefined)),
   {_Good, MalformedP} = lists:splitwith(fun({_N, E}) -> E /= invalid end,
      [{<<"dfs">>, Dfs}, {<<"tags">>, Tags}]),
   Malformed = MalformedP /= [],
   {Malformed, rest_helper:report_malformed(Malformed, Req1, proplists:get_keys(MalformedP)),
      State#state{dfs = Dfs, tags = Tags}};
malformed_request(Req, State=#state{mode = _Mode}) ->
   {false, Req, State}.

%% check for existing resource only with get req
resource_exists(Req = #{method := <<"GET">>}, State=#state{mode = Mode, task_id = TId})
   when Mode == get orelse Mode == get_graph orelse Mode == start orelse Mode == stop ->
   check_resource(TId, Req, State);
resource_exists(Req = #{method := <<"POST">>}, State=#state{mode = Mode, task_id = TId})
   when Mode == remove_tags orelse Mode == add_tags ->
   Res = check_resource(TId, Req, State),
   Res;
resource_exists(Req, State) ->
   {true, Req, State}.

check_resource(TId, Req, State) ->
   {Value, NewState} =
   case TId of
      undefined -> {true, State};
      Id -> case faxe:get_task(Id) of
               {error, not_found} -> {false, State};
               Task=#task{} -> {true, State#state{task = Task, task_id = Task#task.id}}
            end
   end,
   {Value, Req, NewState}.

delete_resource(Req, State=#state{}) ->
   do_delete(Req, State).

%% delete a task-group
do_delete(Req, State=#state{task_id = undefined}) ->
   GroupName = cowboy_req:binding(groupname, Req),
   case faxe:delete_task_group(GroupName) of
      {error, Error} ->
         lager:info("Error occured when deleting group: ~p",[Error]),
         Req3 = cowboy_req:set_resp_body(
            jiffy:encode(#{success => false, error => faxe_util:to_bin(Error)}), Req),
         Req4 = cowboy_req:reply(409, Req3),
         {stop, Req4, State};
      _Other ->
         RespMap = #{success => true, message =>
            iolist_to_binary([<<"Task-Group ">>, GroupName, <<" successfully deleted.">>])},
         Req2 = cowboy_req:set_resp_body(jiffy:encode(RespMap), Req),
         {true, Req2, State}
   end;
%% delete a single task
do_delete(Req, State=#state{task_id = TaskId, delete_force = Force}) ->
   case faxe:delete_task(TaskId, Force) of
      ok ->
         RespMap = #{success => true, message =>
            iolist_to_binary([<<"Task ">>, faxe_util:to_bin(TaskId), <<" successfully deleted.">>])},
         Req2 = cowboy_req:set_resp_body(jiffy:encode(RespMap), Req),
         {true, Req2, State};
      {error, Error} ->
         lager:info("Error occured when deleting flow: ~p",[Error]),
         Req3 = cowboy_req:set_resp_body(
            jiffy:encode(#{success => false, error => faxe_util:to_bin(Error)}), Req),
         Req4 = cowboy_req:reply(409, Req3),
         {stop, Req4, State}
   end.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% costum CALLBACKS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


get_to_json(Req, State=#state{task = Task}) ->
   Map = rest_helper:task_to_map(Task),
   {jiffy:encode(Map), Req, State}.

get_graph_to_json(Req, State=#state{task = Task}) ->
   {jiffy:encode(faxe:get_graph(Task)), Req, State}.

from_register_task(Req, State = #state{name = TaskName, dfs = Dfs, tags = Tags}) ->
   rest_helper:do_register(Req, TaskName, Dfs, Tags, State, task).

from_upsert_task(Req, State = #state{name = TaskId}) ->
   case faxe:get_task(TaskId) of
      {error, not_found} -> from_register_task(Req, State);
      #task{id = TId, name = Name} -> from_update_to_json(Req, State#state{task_id = TId, name = Name})
   end.


from_start_temp_task(Req, State) ->
   {ok, Result, Req3} = cowboy_req:read_urlencoded_body(Req),
%%   TaskName = proplists:get_value(<<"name">>, Result),
   Dfs = proplists:get_value(<<"dfs">>, Result),
   TTL = binary_to_integer(proplists:get_value(<<"ttl">>, Result)),
   case faxe:start_temp(Dfs, TTL) of
      {ok, TaskName} ->
         Req4 = cowboy_req:set_resp_body(jiffy:encode(#{success => true, id => TaskName, ttl => TTL}), Req3),
               {true, Req4, State};
      {error, Error} -> lager:warning("Error occured when starting temporary task: ~p",[Error]),
         Req4 = cowboy_req:set_resp_body(jiffy:encode(#{success => false,
            error => faxe_util:to_bin(Error)}), Req3),
         {false, Req4, State}
   end.

from_update_to_json(Req, State=#state{task_id = TaskId, dfs = Dfs, tags = Tags}) ->
%%   lager:info("update ~p with dfs: ~p~nand tags: ~p",[TaskId, Dfs,Tags]),
   case faxe:update_string_task(Dfs, TaskId) of
      ok ->
         Req4 = cowboy_req:set_resp_body(jiffy:encode(#{success => true, id => TaskId}), Req),
         rest_helper:set_tags(Tags, TaskId),
         {true, Req4, State};
      {error, Error} ->
         Req3 = cowboy_req:set_resp_body(
            jiffy:encode(#{success => false, error => faxe_util:to_bin(Error)}), Req),
         {false, Req3, State}
   end.

from_ping_to_json(Req, State=#state{task_id = TaskId}) ->
   case faxe:ping_task(TaskId) of
      {ok, Time} ->
         Req4 = cowboy_req:set_resp_body(
            jiffy:encode(#{success => true, id => TaskId, ttl => Time}), Req),
         {true, Req4, State};
      {error, Error} ->
         Req3 = cowboy_req:set_resp_body(
            jiffy:encode(#{success => false, error => faxe_util:to_bin(Error)}), Req),
         {false, Req3, State}
   end.

start_to_json(Req, State = #state{task_id = Id}) ->
   case faxe:start_task(Id, is_permanent(Req)) of
      {ok, _Graph} ->
         rest_helper:success(Req, State);
      {error, already_started} ->
         Qs = cowboy_req:parse_qs(Req),
         case lists:keyfind(<<"quiet">>, 1, Qs) of
            {_, <<"true">>} -> rest_helper:success(Req, State);
            _ -> rest_helper:error(Req, State, <<"already_started">>)
         end;
      {error, Error} ->
         rest_helper:error(Req, State, faxe_util:to_bin(Error))
   end.

start_group_to_json(Req, State = #state{task_id = Id}) ->
   Num0 = cowboy_req:binding(concurrency, Req),
   Num = case catch binary_to_integer(Num0) of N when is_integer(N) -> N; _ -> 1 end,
   Mode = #task_modes{concurrency = Num, permanent = is_permanent(Req)},
   case faxe:start_task(Id, Mode) of
      {ok, _Graph} ->
         rest_helper:success(Req, State);
      {error, Error} ->
         rest_helper:error(Req, State, faxe_util:to_bin(Error))
   end.

set_group_size_to_json(Req, State) ->
   NewSize0 = cowboy_req:binding(group_size, Req),
   GroupName = cowboy_req:binding(groupname, Req),
   NewSize = case catch binary_to_integer(NewSize0) of N when is_integer(N) -> N; _ -> 1 end,
   case faxe:set_group_size(GroupName, NewSize) of
      {error, What} ->
         rest_helper:error(Req, State, faxe_util:to_bin(What));
      _Other ->
         SizeBin = integer_to_binary(NewSize),
         rest_helper:success(Req, State, <<"set new group size to ", SizeBin/binary>>)
   end.

start_metrics_trace_to_json(Req, State = #state{task_id = Id}) ->
   TraceDuration = get_duration(Req),
   lager:info("start metrics trace: ~p :: ~p",[Id, TraceDuration]),
   case faxe:start_metrics_trace(Id, TraceDuration) of
      {ok, _Graph} ->
         rest_helper:success(Req, State);
      {error, Error} ->
         rest_helper:error(Req, State, faxe_util:to_bin(Error))
   end.

start_debug_to_json(Req, State = #state{task_id = Id}) ->
   DebugDuration = get_duration(Req),
   lager:info("start debug trace: ~p :: ~p",[Id, DebugDuration]),
   case faxe:start_trace(Id, DebugDuration) of
      {ok, _Graph} ->
         rest_helper:success(Req, State);
      {error, Error} ->
         rest_helper:error(Req, State, faxe_util:to_bin(Error))
   end.

stop_to_json(Req, State = #state{task_id = Id}) ->
   case faxe:stop_task(Id, is_permanent(Req)) of
      ok ->
         rest_helper:success(Req, State);
      {error, Error} ->
         rest_helper:error(Req, State, faxe_util:to_bin(Error))
   end.

stop_group_to_json(Req, State = #state{}) ->
   GroupName = cowboy_req:binding(groupname, Req),
   case faxe:stop_task_group(GroupName, is_permanent(Req)) of
      {error, Error} ->
         rest_helper:error(Req, State, faxe_util:to_bin(Error));
      _Other ->
         rest_helper:success(Req, State)
   end.

stop_metrics_trace_to_json(Req, State = #state{task_id = Id}) ->
   case faxe:stop_metrics_trace(Id) of
      {ok, _Graph} ->
         rest_helper:success(Req, State);
      {error, Error} ->
         rest_helper:error(Req, State, faxe_util:to_bin(Error))
   end.

stop_debug_to_json(Req, State = #state{task_id = Id}) ->
   case faxe:stop_trace(Id) of
      {ok, _Graph} ->
         rest_helper:success(Req, State);
      {error, Error} ->
         rest_helper:error(Req, State, faxe_util:to_bin(Error))
   end.

create_to_json(Req, State) ->
   {stop, Req, State}.

add_tags_from_json(Req, State = #state{task_id = TaskId, tags = Tags}) ->
   case faxe:add_tags(TaskId, Tags) of
      ok ->
         Req4 = cowboy_req:set_resp_body(
            jiffy:encode(#{success => true}), Req),
         {true, Req4, State};
      {error, Error} ->
         Req4 = cowboy_req:set_resp_body(
            jiffy:encode(#{success => false, error => faxe_util:to_bin(Error)}), Req),
         {false, Req4, State}
   end.

remove_tags_from_json(Req, State = #state{task_id = TaskId, tags = Tags}) ->
   case faxe:remove_tags(TaskId, Tags) of
      ok ->
         Req4 = cowboy_req:set_resp_body(
            jiffy:encode(#{success => true}), Req),
         {true, Req4, State};
      {error, Error} ->
         Req4 = cowboy_req:set_resp_body(
            jiffy:encode(#{success => false, error => faxe_util:to_bin(Error)}), Req),
         {false, Req4, State}
   end.

is_permanent(Req) ->
   Permanent = cowboy_req:binding(permanent, Req, <<"false">>),
   Permanent == <<"true">>.


convert_tags(undefined) -> [];
convert_tags(Bin) when is_binary(Bin) ->
   case catch jiffy:decode(Bin) of
      TagList when is_list(TagList) -> TagList;
      _ -> invalid
   end;
convert_tags(_) -> invalid.

-spec get_duration(cowboy_req:req()) -> undefined|non_neg_integer().
get_duration(Req) ->
   DurationMin = cowboy_req:binding(duration_minutes, Req),
   case DurationMin of
      DurMin when is_binary(DurMin) ->
         case catch (binary_to_integer(DurMin)) of
            Minutes when is_integer(Minutes) -> Minutes * 60 * 1000;
            _ -> undefined
         end;
      undefined -> undefined
   end.