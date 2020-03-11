%%%-------------------------------------------------------------------
%%% @author heyoka
%%% @copyright (C) 2020, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 10. Mär 2020 21:16
%%%-------------------------------------------------------------------
-module(faxe_config).
-author("heyoka").

%% API
-export([get/1]).

get(Key) ->
   application:get_env(faxe, Key, undefined).
