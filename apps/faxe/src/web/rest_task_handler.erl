%%%-------------------------------------------------------------------
%%% @author $author
%%% @copyright (C) $year, $company
%%% @doc
%%%
%%% @end
%%% Created : $fulldate
%%%-------------------------------------------------------------------
-module(rest_task_handler).

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
   errors_to_json/2,
   from_ping_to_json/2,
   from_start_temp_task/2,
   remove_tags_from_json/2,
   add_tags_from_json/2
   , logs_to_json/2,
   start_debug_to_json/2,
   stop_debug_to_json/2,
   start_group_to_json/2,
   stop_group_to_json/2,
   set_group_size_to_json/2]).

-include("faxe.hrl").

-record(state, {mode, task_id, task, tags, name, dfs}).

init(Req, [{op, Mode}]) ->
   TId = cowboy_req:binding(task_id, Req),
   TaskId =
   case catch binary_to_integer(TId) of
      I when is_integer(I) -> I;
      _ -> TId
   end,
   {cowboy_rest, Req, #state{mode = Mode, task_id = TaskId}}.

is_authorized(Req, State) ->
   rest_helper:is_authorized(Req, State).

allowed_methods(Req, State=#state{mode = get}) ->
   {[<<"GET">>, <<"OPTIONS">>], Req, State};
allowed_methods(Req, State=#state{mode = get_graph}) ->
   {[<<"GET">>, <<"OPTIONS">>], Req, State};
allowed_methods(Req, State=#state{mode = register}) ->
   {[<<"POST">>], Req, State};
allowed_methods(Req, State=#state{mode = start_temp}) ->
   {[<<"POST">>], Req, State};
allowed_methods(Req, State=#state{mode = start_debug}) ->
   {[<<"GET">>], Req, State};
allowed_methods(Req, State=#state{mode = stop_debug}) ->
   {[<<"GET">>], Req, State};
allowed_methods(Req, State=#state{mode = update}) ->
   {[<<"POST">>], Req, State};
allowed_methods(Req, State=#state{mode = ping}) ->
   {[<<"POST">>], Req, State};
allowed_methods(Req, State=#state{mode = start}) ->
   {[<<"GET">>, <<"OPTIONS">>], Req, State};
allowed_methods(Req, State=#state{mode = start_group}) ->
   {[<<"GET">>, <<"OPTIONS">>], Req, State};
allowed_methods(Req, State=#state{mode = stop}) ->
   {[<<"GET">>, <<"OPTIONS">>], Req, State};
allowed_methods(Req, State=#state{mode = stop_group}) ->
   {[<<"GET">>, <<"OPTIONS">>], Req, State};
allowed_methods(Req, State=#state{mode = stats}) ->
   {[<<"GET">>, <<"OPTIONS">>], Req, State};
allowed_methods(Req, State=#state{mode = errors}) ->
   {[<<"GET">>, <<"OPTIONS">>], Req, State};
allowed_methods(Req, State=#state{mode = logs}) ->
   {[<<"GET">>, <<"OPTIONS">>], Req, State};
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
content_types_provided(Req, State=#state{mode = stop}) ->
   {[
      {{<<"application">>, <<"json">>, []}, stop_to_json},
      {{<<"text">>, <<"html">>, []}, stop_to_json}
   ], Req, State};
content_types_provided(Req, State=#state{mode = stop_group}) ->
   {[
      {{<<"application">>, <<"json">>, []}, stop_group_to_json}
   ], Req, State};
content_types_provided(Req, State=#state{mode = set_group_size}) ->
   {[
      {{<<"application">>, <<"json">>, []}, set_group_size_to_json}
   ], Req, State};
content_types_provided(Req0 = #{method := _Method}, State=#state{mode = errors}) ->
   {[
      {{<<"application">>, <<"json">>, []}, errors_to_json},
      {{<<"text">>, <<"html">>, []}, errors_to_json}
   ], Req0, State};
content_types_provided(Req0 = #{method := _Method}, State=#state{mode = logs}) ->
   {[
      {{<<"application">>, <<"json">>, []}, logs_to_json}
   ], Req0, State};
content_types_provided(Req0 = #{method := _Method}, State=#state{mode = _Mode}) ->
   {[
      {{<<"application">>, <<"json">>, []}, create_to_json},
      {{<<"text">>, <<"html">>, []}, create_to_json}
   ], Req0, State}.
%%.

malformed_request(Req, State=#state{mode = Mode}) when Mode == add_tags; Mode == remove_tags ->
   {ok, Result, Req1} = cowboy_req:read_urlencoded_body(Req),
   TagsJson = proplists:get_value(<<"tags">>, Result, undefined),
   Malformed = TagsJson == undefined,
   {Malformed,
      rest_helper:report_malformed(Malformed, Req1, [<<"tags">>]), State#state{tags = TagsJson}};
malformed_request(Req, State=#state{mode = Mode}) when Mode == register ->
   {ok, Result, Req1} = cowboy_req:read_urlencoded_body(Req),
   Dfs = proplists:get_value(<<"dfs">>, Result, undefined),
   Name = proplists:get_value(<<"name">>, Result, undefined),
   Tags = proplists:get_value(<<"tags">>, Result, []),
   Malformed = (Dfs == undefined orelse Name == undefined),
   {Malformed, rest_helper:report_malformed(Malformed, Req1, [<<"dfs">>, <<"name">>]),
      State#state{dfs = Dfs, name = Name, tags = Tags}};
malformed_request(Req, State=#state{mode = Mode}) when Mode == update ->
   {ok, Result, Req1} = cowboy_req:read_urlencoded_body(Req),
   Dfs = proplists:get_value(<<"dfs">>, Result, undefined),
   Tags = proplists:get_value(<<"tags">>, Result, []),
   Malformed = (Dfs == undefined),
   {Malformed, rest_helper:report_malformed(Malformed, Req1, [<<"dfs">>]),
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
         {false, Req3, State};
      _Other ->
         RespMap = #{success => true, message =>
         iolist_to_binary([<<"Task-Group ">>, GroupName, <<" successfully deleted.">>])},
         Req2 = cowboy_req:set_resp_body(jiffy:encode(RespMap), Req),
         {true, Req2, State}
   end;
%% delete a single task
do_delete(Req, State=#state{task_id = TaskId}) ->
   case faxe:delete_task(TaskId) of
      ok ->
         RespMap = #{success => true, message =>
         iolist_to_binary([<<"Task ">>, faxe_util:to_bin(TaskId), <<" successfully deleted.">>])},
         Req2 = cowboy_req:set_resp_body(jiffy:encode(RespMap), Req),
         {true, Req2, State};
      {error, Error} ->
         lager:info("Error occured when deleting flow: ~p",[Error]),
         Req3 = cowboy_req:set_resp_body(
            jiffy:encode(#{success => false, error => faxe_util:to_bin(Error)}), Req),
         {false, Req3, State}
   end.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% costum CALLBACKS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


get_to_json(Req, State=#state{task = Task}) ->
   Map = rest_helper:task_to_map(Task),
   {jiffy:encode(Map), Req, State}.

get_graph_to_json(Req, State=#state{task = Task}) ->
   {jiffy:encode(faxe:task_to_graph(Task)), Req, State}.

from_register_task(Req, State = #state{name = TaskName, dfs = Dfs, tags = Tags}) ->
   rest_helper:do_register(Req, TaskName, Dfs, Tags, State, task).

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
         {jiffy:encode(#{<<"ok">> => <<"started">>}), Req, State};
      {error, Error} ->
         {jiffy:encode(#{<<"error">> => faxe_util:to_bin(Error)}), Req, State}
   end.

start_group_to_json(Req, State = #state{task_id = Id}) ->
   Num0 = cowboy_req:binding(concurrency, Req),
   Num = case catch binary_to_integer(Num0) of N when is_integer(N) -> N; _ -> 1 end,
   Mode = #task_modes{concurrency = Num, permanent = is_permanent(Req)},
   case faxe:start_task(Id, Mode) of
      {ok, _Graph} ->
         {jiffy:encode(#{<<"ok">> => <<"group started">>}), Req, State};
      {error, Error} ->
         {jiffy:encode(#{<<"error">> => faxe_util:to_bin(Error)}), Req, State}
   end.

set_group_size_to_json(Req, State) ->
   NewSize0 = cowboy_req:binding(group_size, Req),
   GroupName = cowboy_req:binding(groupname, Req),
   NewSize = case catch binary_to_integer(NewSize0) of N when is_integer(N) -> N; _ -> 1 end,
   case faxe:set_group_size(GroupName, NewSize) of
      {error, What} ->
         {jiffy:encode(#{<<"error">> => faxe_util:to_bin(What)}), Req, State};
      _Other ->
         SizeBin = integer_to_binary(NewSize),
         {jiffy:encode(#{<<"ok">> => <<"set new group size to ", SizeBin/binary>>}), Req, State}
   end.


start_debug_to_json(Req, State = #state{task_id = Id}) ->
%%   lager:info("start trace: ~p",[Id]),
   case faxe:start_trace(Id) of
      {ok, _Graph} ->
         {jiffy:encode(#{<<"ok">> => <<"debug_started">>}), Req, State};
      {error, Error} ->
         {jiffy:encode(#{<<"error">> => faxe_util:to_bin(Error)}), Req, State}
   end.

stop_to_json(Req, State = #state{task_id = Id}) ->
   case faxe:stop_task(Id, is_permanent(Req)) of
      ok ->
         {jiffy:encode(#{<<"ok">> => <<"stopped">>}), Req, State};
      {error, Error} ->
         {jiffy:encode(#{<<"error">> => faxe_util:to_bin(Error)}), Req, State}
   end.

stop_group_to_json(Req, State = #state{}) ->
   GroupName = cowboy_req:binding(groupname, Req),
   case faxe:stop_task_group(GroupName, is_permanent(Req)) of
      {error, Error} ->
         {jiffy:encode(#{<<"error">> => faxe_util:to_bin(Error)}), Req, State};
      _Other ->
         {jiffy:encode(#{<<"ok">> => <<"group stopped">>}), Req, State}
   end.

stop_debug_to_json(Req, State = #state{task_id = Id}) ->
   case faxe:stop_trace(Id) of
      {ok, _Graph} ->
         {jiffy:encode(#{<<"ok">> => <<"debug_stopped">>}), Req, State};
      {error, Error} ->
         {jiffy:encode(#{<<"error">> => faxe_util:to_bin(Error)}), Req, State}
   end.

errors_to_json(Req, State = #state{task_id = Id}) ->
   case faxe:get_errors(Id) of
      {ok, Errors} ->
         {jiffy:encode(#{<<"error">> => faxe_util:to_bin(Errors)}), Req, State};
      {error, What} ->
         {jiffy:encode(#{<<"error">> => faxe_util:to_bin(What)}), Req, State}
   end.

%% read log from crate db
logs_to_json(Req, State = #state{task_id = Id}) ->
   #{max_age := MaxAge, limit := Limit} =
      cowboy_req:match_qs([{max_age, [], <<"15">>}, {limit, [], <<"20">>}], Req),
   case faxe:get_logs(Id, "",
      binary_to_integer(MaxAge)*60*1000, binary_to_integer(Limit)
   ) of
      {ok, Logs} ->
         {jiffy:encode(#{<<"logs">> => Logs}), Req, State};
      {error, What} ->
         {jiffy:encode(#{<<"error">> => faxe_util:to_bin(What)}), Req, State}
   end.

create_to_json(Req, State) ->
   {stop, Req, State}.

add_tags_from_json(Req, State = #state{task_id = TaskId, tags = TagsJson}) ->
   Tags = jiffy:decode(TagsJson),
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

remove_tags_from_json(Req, State = #state{task_id = TaskId, tags = TagsJson}) ->
   Tags = jiffy:decode(TagsJson),
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
