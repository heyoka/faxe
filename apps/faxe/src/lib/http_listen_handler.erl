%%%-------------------------------------------------------------------
%%% @author $author
%%% @copyright (C) $year, $company
%%% @doc
%%%
%%% @end
%%% Created : $fulldate
%%%-------------------------------------------------------------------
-module(http_listen_handler).

-include("faxe.hrl").
%%
%% Cowboy callbacks
-export([
   init/2,
   allowed_methods/2,
   is_authorized/2,
   content_types_accepted/2,
   malformed_request/2,
   from_req/2, allow_missing_post/2]).

%%
%% Additional callbacks
-export([
]).

-record(state, {client_pid, content_type, body, peer, user, pass, body_length}).

init(#{peer := Peer} = Req, [{client, Pid}, {content_type, ContentType}, {user, User}, {pass, Pass}]) ->
   Length = cowboy_req:header(<<"content-length">>, Req, <<"0">>),
   {cowboy_rest, Req,
      #state{
         client_pid = Pid,
         peer = Peer,
         content_type = ContentType,
         user = User,
         pass = Pass,
         body_length = binary_to_integer(Length)}}.

is_authorized(Req, State = #state{user = undefined}) ->
   {true, Req, State};
is_authorized(Req, State = #state{user = User, pass = Pass}) ->
   case cowboy_req:parse_header(<<"authorization">>, Req) of
      {basic, User , Pass} ->
         {true, Req, State};
      _ ->
         {{false, <<"Basic realm=\"faxe\"">>}, Req, State}
   end.

allowed_methods(Req, State=#state{}) ->
   {[<<"POST">>, <<"PUT">>], Req, State}.

content_types_accepted(Req, State = #state{content_type = CType}) ->
   Value = [{CType, from_req}],
   {Value, Req, State}.

allow_missing_post(Req, State) ->
   {false, Req, State}.

malformed_request(Req, State = #state{content_type = {<<"text">>, <<"plain">>, _}}) ->
   {ok, Body, Req1} = cowboy_req:read_body(Req),
   {false, Req1, State#state{body = Body}};
malformed_request(Req, State = #state{content_type = {<<"application">>, <<"json">>, _}}) ->
   {ok, Body, Req1} = cowboy_req:read_body(Req),
   {false, Req1, State#state{body = Body}};
malformed_request(Req, State = #state{content_type = {<<"application">>, <<"x-www-form-urlencoded">>, _}}) ->
   {ok, Result, Req1} = cowboy_req:read_urlencoded_body(Req),
   {false, Req1, State#state{body = Result}};
malformed_request(Req, State) ->
   {true, Req, State}.


from_req(Req, State = #state{client_pid = ClientPid, body = Body, body_length = Length}) ->
   ClientPid ! {http_data, Body, Length},
   RespMap = #{<<"success">> => true},
   Req2 = cowboy_req:set_resp_header(<<"content-type">>, <<"application/json">>, Req),
   Req3 = cowboy_req:set_resp_body(jiffy:encode(RespMap), Req2),
   {true, Req3, State}.

