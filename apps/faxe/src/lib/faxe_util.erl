%%%-------------------------------------------------------------------
%%% @author heyoka
%%% @copyright (C) 2019, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 09. Dec 2019 09:57
%%%-------------------------------------------------------------------
-module(faxe_util).
-author("heyoka").

%% API
-export([uuid_string/0]).

uuid_string() ->
   uuid:uuid_to_string(uuid:get_v4(strong)).
