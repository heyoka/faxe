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
-export([user_agent/0]).

-spec user_agent() -> iodata().
user_agent() ->
  {ok, Vsn} = application:get_key(vsn),
  [<<"faxe/">>, Vsn].
