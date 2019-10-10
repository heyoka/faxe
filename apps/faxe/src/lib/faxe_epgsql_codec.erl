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

%% API
-export([init/2, names/0, encode/3, decode/3]).


init(_, _Sock) ->
   State = undefined,
   State.

names() ->
   [time, timestamp, timestamptz].

encode(Data, _, _State) ->
   Data.

decode(<<Ts:64>>, _, _State) ->
   Ts;
decode(Ts, _, _State) when is_integer(Ts) ->
   Ts*1000;
decode({Date, {Hour, Minute, SecondFrac}}, timestamp, _State) ->
   Second = erlang:trunc(SecondFrac),
   M0 = SecondFrac - Second,
   Milli = erlang:round(M0 * 1000),
   faxe_time:to_ms({Date, {Hour, Minute, Second, Milli}}).
