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


execute(Req, Env) ->
  Req1 = cowboy_req:set_resp_header(<<"access-control-allow-origin">>, <<"*">>, Req),
  {ok, Req1, Env}.