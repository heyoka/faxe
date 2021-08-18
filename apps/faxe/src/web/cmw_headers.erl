%%%-------------------------------------------------------------------
%%% @author heyoka
%%% @copyright (C) 2020
%%%
%%% @end
%%% Created : 13. May 2020 14:51
%%%-------------------------------------------------------------------
-module(cmw_headers).
-author("heyoka").

-behavior(cowboy_middleware).

%% API
-export([execute/2]).

%% on the one hand we add an OPTIONS method to all endpoints implicitely,
%% on the other hand we set cors headers when OPTIONS is used
execute(#{method := <<"OPTIONS">>} = Req, Env) ->
  #{handler := Handler, handler_opts := HandlerOpts} = Env,
  {cowboy_rest, _Req, State} = Handler:init(Req, HandlerOpts),
  {AllowedMethods, _ , _} = Handler:allowed_methods(Req, State),
  {ok, ReqWithCorsHeaders} = set_cors_headers(Req, AllowedMethods),
  ReqFinal = cowboy_req:reply(200, ReqWithCorsHeaders),
  {stop, ReqFinal};
execute(Req, Env) ->
  Req2 = cowboy_req:set_resp_header(<<"access-control-allow-origin">>, <<"*">>, Req),
  {ok, Req2, Env}.

set_cors_headers(Req, Methods) ->
  Headers = [
    {<<"access-control-allow-origin">>, <<"*">>},
    {<<"access-control-allow-methods">>, lists:join(<<", ">>, Methods ++ [<<"OPTIONS">>])},
    {<<"access-control-allow-headers">>, <<"*">>},
    {<<"access-control-max-age">>, <<"900">>}],
  set_headers(Headers, Req).

set_headers(Headers, Req) ->
  ReqWithHeaders = lists:foldl(
      fun({Header, Value}, ReqIn) ->
        cowboy_req:set_resp_header(Header, Value, ReqIn)
      end, Req, Headers),
  {ok, ReqWithHeaders}.

