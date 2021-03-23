%%%-------------------------------------------------------------------
%%% @author heyoka
%%% @copyright (C) 2019, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 20. Aug 2019 16:02
%%%-------------------------------------------------------------------
-module(rest_helper).
-author("heyoka").

-include("faxe.hrl").

%% API
-export([task_to_map/1, template_to_map/1, do_register/6,
   reg_fun/3, report_malformed/3, add_tags/2, set_tags/2,
   get_task_or_template_id/2, is_authorized/1, is_authorized/2]).


-spec is_authorized(term()) -> {true, Username::binary()} | false.
is_authorized(Req) ->
   case cowboy_req:parse_header(<<"authorization">>, Req) of
      {basic, User , Pass} ->
         case faxe_db:has_user_with_pw(User, Pass) of
            true -> rest_audit_server:audit(User, Req), {true, User};
            false -> lager:notice("user: ~p with pw: ~p is not authorized", [User, Pass]), false
         end;
      _ ->
         case faxe_config:get(allow_anonymous, false) of
            true ->
               User = <<"anon">>,
               rest_audit_server:audit(User, Req), {true, User};
            false -> false
         end
   end.

is_authorized(Req, State) ->
   case rest_helper:is_authorized(Req) of
      {true, _User} -> {true, Req, State};
      false -> {{false, <<"Basic realm=\"faxe\"">>}, Req, State}
   end.


task_to_map(_T = #task{
   id = Id, name = Name, date = Dt, is_running = Running,
   last_start = LStart, last_stop = LStop, dfs = Dfs, permanent = Perm,
   template = Template, template_vars = TemplateVars, tags = Tags,
   group = Group, group_leader = Leader
}) ->
   Map = #{
      <<"id">> => Id,
      <<"name">> => Name,
      <<"dfs">> => Dfs,
      <<"running">> => Running,
      <<"permanent">> => Perm,
      <<"changed">> => faxe_time:to_iso8601(Dt),
      <<"last_start">> => faxe_time:to_iso8601(LStart),
      <<"last_stop">> => faxe_time:to_iso8601(LStop),
      <<"tags">> => Tags,
      <<"group">> => Group,
      <<"group_leader">> => Leader
   },
   OutMap =
   case Template of
      <<>> -> Map;
      _ -> Map#{<<"template">> => Template, <<"template_vars">> => TemplateVars}
   end,
   OutMap.

template_to_map(_T = #template{definition = _Def0, id = Id, name = Name, date = Dt, dfs = Dfs}) ->
   Map = #{
      <<"id">> => Id,
      <<"name">> => Name,
      <<"dfs">> => Dfs,
      <<"changed">> => faxe_time:to_iso8601(Dt)},
   Map.

report_malformed(false, Req, _) -> Req;
report_malformed(true, Req, ParamList) ->
   Req1 = cowboy_req:set_resp_header(<<"Content-Type">>, <<"application/json">>, Req),
   cowboy_req:set_resp_body(jiffy:encode(#{success => false, params_missing => ParamList}), Req1).

do_register(Req, TaskName, Dfs, Tags, State, Type) ->
   case reg_fun(Dfs, TaskName, Type) of
      ok ->
         Id = get_task_or_template_id(TaskName, Type),
         case Type of
            task -> add_tags(Tags, Id);
            _ -> ok
         end,
         Req2 = cowboy_req:set_resp_body(
            jiffy:encode(#{success => true, name => TaskName, id => Id}), Req),
         {true, Req2, State};
      {error, Error} ->
         Add =
            case Type of
               task -> "";
               _ -> "-template"
            end,
         lager:warning("Error occured when registering faxe-flow"++Add++": ~p",[Error]),
         Req4 = cowboy_req:set_resp_body(jiffy:encode(#{success => false, error => faxe_util:to_bin(Error)}), Req),
         {false, Req4, State}
   end.

reg_fun(Dfs, Name, task) -> Res = faxe:register_string_task(Dfs, Name), Res;
reg_fun(Dfs, Name, _) -> faxe:register_template_string(Dfs, Name).

-spec add_tags(list(), any()) -> ok | {error, term()}.
add_tags(Tags, TaskId) ->
   case Tags of
      [] -> ok;
      TagJson ->
         faxe:add_tags(TaskId, jiffy:decode(TagJson))
   end.

-spec set_tags(list(), any()) -> ok | {error, term()}.
set_tags(Tags, TaskId) ->
   Tags1 =
   case Tags of
      [] -> [];
      _ -> jiffy:decode(Tags)
   end,
   faxe:set_tags(TaskId, Tags1).

get_task_or_template_id(TName, task) ->
   NewTask = faxe:get_task(TName),
      case NewTask of
         #task{id = NewId} -> NewId;
         _  -> 0
      end;
get_task_or_template_id(TName, _) ->
   NewTask = faxe:get_template(TName),
      case NewTask of
         #template{id = NewId} -> NewId;
         _  -> 0
      end.