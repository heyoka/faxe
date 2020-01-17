%%%-------------------------------------------------------------------
%%% @author heyoka
%%% @copyright (C) 2019, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 01. Aug 2019 16:53
%%%-------------------------------------------------------------------
-module(tcp_msg_parser).
-author("heyoka").

-include("faxe.hrl").
%% API
-export([convert/3, convert/4]).

-callback parse(BinData :: binary()) -> map().

%% @doc parse and convert binary-data
-spec convert(Data :: binary(), binary(), Parser :: atom()) -> #data_point{}.
convert(Data, As, undefined) ->
   NewPoint = flowdata:set_field(#data_point{ts = faxe_time:now()}, As, Data),
   NewPoint;
convert(Data, As, Parser) ->
   convert(Data, #data_point{ts = faxe_time:now()}, As, Parser).
convert(Data, Point = #data_point{}, As, Parser) ->
   {T, NPoint, D} =
   case timer:tc(Parser, parse, [Data]) of
      {Time, {_DataFormat, _Vers, _Map}=D0} -> {Time, Point, D0};
      {Time, {Ts, DataFormat, Vers, Map}} -> {Time, Point#data_point{ts=Ts}, {DataFormat, Vers, Map}}
   end,
%%   {T, {DataFormat, Vers, Map}} = timer:tc(Parser, parse, [Data]),
   lager:notice("Parser time <~p>: ~p",[Parser, T]),
   {DataF, Vs, Mp} = D,
   P0 = flowdata:set_field(NPoint, As, Mp),
   P1 = flowdata:set_field(P0, <<"df">>, DataF),
   flowdata:set_field(P1, <<"vs">>, Vs).

%%%%%%%
