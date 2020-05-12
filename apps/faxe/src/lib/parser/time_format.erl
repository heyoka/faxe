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

-include("timeformats.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% API
-export([convert/2]).
%% to faxe "format"
-export([
   float_micro_to_ms/1,
   millisecond_to_ms/1,
   second_to_ms/1,
   float_millisecond_to_ms/1,
   rfc3339_to_ms/1,
   iso8601_to_ms/1,
   conv_dt_to_ms/1]).

%% from faxe "format"
-export([
   to_rfc3339/1
   , to_iso8601/1]).

convert(Input, ?TF_TS_MILLI) ->
   millisecond_to_ms(Input);
convert(Input, ?TF_TS_SECOND) ->
   second_to_ms(Input);
convert(Input, ?TF_TS_FLOAT_MICRO) ->
   float_micro_to_ms(Input);
convert(Input, ?TF_TS_FLOAT_MILLI) ->
   float_millisecond_to_ms(Input);
convert(Input, ?TF_ISO8601) ->
   iso8601_to_ms(Input);
convert(Input, ?TF_RFC3339) ->
   rfc3339_to_ms(Input);
convert(Input, ?TF_CONVTRACK_DT) ->
   conv_dt_to_ms(Input).


%% <<"1565343079.173588">>
-spec float_micro_to_ms(binary()) -> faxe_time:timestamp().
float_micro_to_ms(BinString) when is_binary(BinString)->
   F = binary_to_float(BinString),
   float_micro_to_ms(F);
float_micro_to_ms(Float) when is_float(Float) ->
   erlang:round(Float*1000).

%% <<"1565343079.173">>
float_millisecond_to_ms(BinString) when is_binary(BinString) ->
   float_millisecond_to_ms(binary_to_float(BinString));
float_millisecond_to_ms(Float) when is_float(Float) ->
   erlang:round(Float*1000).

%% 1565343079
second_to_ms(BinString) when is_binary(BinString) ->
   second_to_ms(binary_to_integer(BinString));
second_to_ms(Int) when is_integer(Int) ->
   Int*1000.

%% 1565343079173
millisecond_to_ms(BinString) when is_binary(BinString) ->
   millisecond_to_ms(binary_to_integer(BinString));
millisecond_to_ms(Int) when is_integer(Int) ->
   Int.

%% @doc note: when the iso string is not well formatted, you will lose millisecond precision
%%
%% 2011-10-05T14:48:00.000Z
iso8601_to_ms(IsoString) when is_binary(IsoString) ->
   %% we try rfc3339 first, cause the format is very close and well formatted iso strings
   %% should be handled correctly
   case (catch rfc3339_to_ms(IsoString)) of
      Res when is_integer(Res) -> Res;
      _ -> qdate:to_unixtime(IsoString)*1000
   end.


%% "2018-02-01 15:18:02.088Z"
rfc3339_to_ms(RFCString) when is_binary(RFCString) ->
   rfc3339_to_ms(binary_to_list(RFCString));
rfc3339_to_ms(RFCString) ->
   calendar:rfc3339_to_system_time(RFCString, [{unit, millisecond}]).

%%<<"19.08.01  17:33:44,867  ">>
conv_dt_to_ms(Bin) ->
   F = fun(E) -> binary_to_integer(E) end,
   [Year, Month, Day, H, M, Sec, Ms] =
   lists:map(F, binary:split(Bin, [<<"  ">>, <<".">>, <<":">>, <<",">>], [global,trim])),
   faxe_time:to_ms({{Year+2000,Month,Day},{H,M,Sec,Ms}}).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% FAXE Time (millisecond) to other
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec to_rfc3339(non_neg_integer()) -> binary().
to_rfc3339(Ts) when is_integer(Ts) ->
   to_format(Ts, [{unit, millisecond}, {time_designator, $\s}, {offset, "Z"}]).
-spec to_iso8601(non_neg_integer()) -> binary().
to_iso8601(Ts) when is_integer(Ts) ->
   to_format(Ts, [{unit, millisecond}, {time_designator, $T}, {offset, "Z"}]).
-spec to_format(non_neg_integer(), list()) -> binary().
to_format(Ts, Opts) ->
   list_to_binary(
      calendar:system_time_to_rfc3339(Ts, Opts)
   ).


-ifdef(TEST).
second_to_ms_test() ->
   ?assertEqual(1565343079000, second_to_ms(1565343079)).
float_millisecond_to_ms_test() ->
   ?assertEqual(1565343079173, float_millisecond_to_ms(1565343079.173)).
float_micro_to_ms_test() ->
   ?assertEqual(1565343079174, float_micro_to_ms(<<"1565343079.173588">>)).
float_micro_to_ms2_test() ->
   ?assertEqual(1574067119038, float_micro_to_ms(<<"1574067119.037648">>)).
iso8601_to_ms_test() ->
   Dt = <<"2011-10-05T14:48:00.000Z">>,
   ?assertEqual(1317826080000, iso8601_to_ms(Dt)).
rfc3339_to_ms_test() ->
   Dt = <<"2018-02-01 15:18:02.088Z">>,
   ?assertEqual(1517498282088, rfc3339_to_ms(Dt)).
conv_dt_to_ms_test() ->
   Dt = <<"19.08.01  17:33:44,867  ">>,
   ?assertEqual(1564680824867, conv_dt_to_ms(Dt)).
-endif.


