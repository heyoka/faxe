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
%%   content_types_provided/2,
   is_authorized/2,
   content_types_accepted/2,
   malformed_request/2,
   from_req/2, allow_missing_post/2]).

%%
%% Additional callbacks
-export([
]).

-record(state, {client_pid, content_type, body, peer, user}).

init(#{peer := Peer} = Req, [{client, Pid}, {content_type, ContentType}]) ->
   {cowboy_rest, Req, #state{client_pid = Pid, peer = Peer, content_type = ContentType}}.

is_authorized(Req, State) ->
   {true, Req, State}.

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
malformed_request(Req, State = #state{content_type = {<<"application">>, <<"x-www-form-urlencoded">>, _}}) ->
   {ok, Result, Req1} = cowboy_req:read_urlencoded_body(Req),
   {false, Req1, State#state{body = Result}};
malformed_request(Req, State) ->
   {true, Req, State}.


from_req(Req, State = #state{client_pid = ClientPid, body = Body}) ->
   lager:notice("send ~p to ~p",[Body, ClientPid]),
   ClientPid ! {http_data, Body},
%%   lager:notice("~p got response: ~p", [?MODULE, Res]),
   RespMap = #{<<"success">> => true},
   Req2 = cowboy_req:set_resp_header(<<"content-type">>, <<"application/json">>, Req),
   Req3 = cowboy_req:set_resp_body(jiffy:encode(RespMap), Req2),
   {true, Req3, State}.

