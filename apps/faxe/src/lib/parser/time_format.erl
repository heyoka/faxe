%%%-------------------------------------------------------------------
%%% @author heyoka
%%% @copyright (C) 2019, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 25. Sep 2019 10:39
%%%-------------------------------------------------------------------
-module(time_format).
-author("heyoka").

%% API
-export([float_micro_to_ms/1]).

%% <<"1565343079.173588">>
-spec float_micro_to_ms(binary()) -> faxe_time:timestamp().
float_micro_to_ms(BinString) ->
   F = binary_to_float(BinString),
   erlang:round(F*1000).

