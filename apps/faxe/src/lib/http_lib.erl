%%%-------------------------------------------------------------------
%%% @author heyoka
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 13. Sep 2021 19:58
%%%-------------------------------------------------------------------
-module(http_lib).
-author("heyoka").

%% API
-export([user_agent/0, basic_auth_header/2, user_agent_header/0]).

-spec user_agent() -> iodata().
user_agent() ->
  {ok, Vsn} = application:get_key(vsn),
  [<<"faxe/">>, Vsn].

user_agent_header() ->
  [{<<"user-agent">>, user_agent()}].

basic_auth_header(undefined, _) -> [];
basic_auth_header(_, undefined) -> [];
basic_auth_header(User, Pass) when is_binary(User), is_binary(Pass) ->
  Basic = base64:encode(<<"Basic ", User/binary, ":", Pass/binary>>),
  [{"authorization", Basic}].

