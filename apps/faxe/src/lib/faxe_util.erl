%%%-------------------------------------------------------------------
%%% @author heyoka
%%% @copyright (C) 2019, <COMPANY>
%%% @doc
%%% some utility functions
%%% @end
%%% Created : 09. Dec 2019 09:57
%%%-------------------------------------------------------------------
-module(faxe_util).
-author("heyoka").

%% API
-export([uuid_string/0, round_float/2]).

uuid_string() ->
   uuid:uuid_to_string(uuid:get_v4(strong)).

-spec round_float(float(), non_neg_integer()) -> float().
round_float(Float, Precision) when is_float(Float), is_integer(Precision) ->
   list_to_float(io_lib:format("~.*f",[Precision, Float])).
