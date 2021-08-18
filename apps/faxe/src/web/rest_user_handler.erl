%%%-------------------------------------------------------------------
%%% @author $author
%%% @copyright (C) $year, $company
%%% @doc
%%%
%%% @end
%%% Created : $fulldate
%%%-------------------------------------------------------------------
-module(rest_user_handler).

-include("faxe.hrl").
%%
%% Cowboy callbacks
-export([
   init/2,
   allowed_methods/2,
   content_types_provided/2,
   is_authorized/2,
   content_types_accepted/2,
   user_list_json/2,
   malformed_request/2,
   from_user_add/2, delete_resource/2]).

%%
%% Additional callbacks
-export([
]).

-record(state, {mode, user}).

init(Req, [{op, Mode}]) ->
   {cowboy_rest, Req, #state{mode = Mode}}.

is_authorized(Req, State) ->
   rest_helper:is_authorized(Req, State).

allowed_methods(Req, State=#state{mode = user_delete}) ->
   {[<<"DELETE">>], Req, State};
allowed_methods(Req, State=#state{mode = user_add}) ->
   {[<<"POST">>, <<"PUT">>], Req, State};
allowed_methods(Req, State) ->
    Value = [<<"GET">>],
    {Value, Req, State}.

content_types_accepted(Req, State = #state{mode = user_add}) ->
   Value = [{{ <<"application">>, <<"x-www-form-urlencoded">>, []}, from_user_add}],
   {Value, Req, State}.

content_types_provided(Req, State = #state{mode = user_list}) ->
    {[
       {{<<"application">>, <<"json">>, []}, user_list_json}
    ], Req, State};
content_types_provided(Req0, State) ->
   {[
      {{<<"application">>, <<"json">>, []}, create_to_json},
      {{<<"text">>, <<"html">>, []}, create_to_json}
   ], Req0, State}.

malformed_request(Req, State=#state{mode = user_add}) ->
   {ok, Result, Req1} = cowboy_req:read_urlencoded_body(Req),
   Name = proplists:get_value(<<"name">>, Result, undefined),
   Pass = proplists:get_value(<<"pass">>, Result, []),
   Role = admin,%proplists:get_value(<<"role">>, Result, []),
   Malformed = (Name == undefined orelse Pass == undefined),
   {Malformed, rest_helper:report_malformed(Malformed, Req1, [<<"name">>, <<"pass">>]),
      State#state{user = #faxe_user{name = Name, pw = Pass, role = Role}}};
malformed_request(Req, State) ->
   {false, Req, State}.

delete_resource(Req, State=#state{mode = user_delete}) ->
   User = cowboy_req:binding(username, Req),
   case faxe_db:delete_user(User) of
      ok ->
         RespMap = #{success => true, message =>
         iolist_to_binary([<<"User ">>, faxe_util:to_bin(User), <<" successfully deleted.">>])},
         Req2 = cowboy_req:set_resp_body(jiffy:encode(RespMap), Req),
         {true, Req2, State};
      {error, Error} ->
         lager:info("Error occured when deleting user: ~p",[Error]),
         Req3 = cowboy_req:set_resp_body(
            jiffy:encode(#{success => false, error => faxe_util:to_bin(Error)}), Req),
         {false, Req3, State}
   end.

from_user_add(Req, State = #state{user = User}) ->
   case faxe_db:save_user(User) of
      ok ->
         RespMap = #{success => true},
         Req2 = cowboy_req:set_resp_body(jiffy:encode(RespMap), Req),
         {true, Req2, State};
      {error, What} ->
         Req3 = cowboy_req:set_resp_body(
            jiffy:encode(#{success => false, error => faxe_util:to_bin(What)}), Req),
         {false, Req3, State}
   end.

%% faxe's config hand picked
user_list_json(Req, State=#state{}) ->
   Users = faxe_db:list_users(),
   Out = [user_to_map(U) || U <- Users],
   {jiffy:encode(Out, [uescape]), Req, State}.

user_to_map(#faxe_user{name = Name, pw = PW, role = Role}) ->
   #{<<"name">> => Name, <<"pass">> => PW, <<"role">> => Role}.