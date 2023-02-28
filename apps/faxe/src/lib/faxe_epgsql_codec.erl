%%%-------------------------------------------------------------------
%%% @author heyoka
%%% @copyright (C) 2019, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 10. Oct 2019 08:36
%%%-------------------------------------------------------------------
-module(faxe_epgsql_codec).
-author("heyoka").

-behavior(epgsql_codec).

-define(POSTGRESQL_GS_EPOCH, 63113904000). % calendar:datetime_to_gregorian_seconds({{2000,1,1}, {0,0,0}}).
-define(int64, 1/big-signed-unit:64).

%% API
-export([init/2, names/0, encode/3, decode/3]).


init(_, _Sock) ->
   State = undefined,
   State.

names() ->
   [timestamp].

encode(Data, _, _State) ->
   Data.

decode(<<Ts:?int64>> = _T, timestamp, _State) ->
  TsOutMicro = Ts + (?POSTGRESQL_GS_EPOCH * 1000000) - (62167219200 * 1000000),
  round(TsOutMicro / 1000);
decode({Date, {Hour, Minute, SecondFrac}}=T, timestamp, _State) ->
   Second = erlang:trunc(SecondFrac),
   M0 = SecondFrac - Second,
   Milli = erlang:round(M0 * 1000),
   faxe_time:to_ms({Date, {Hour, Minute, Second, Milli}}).
