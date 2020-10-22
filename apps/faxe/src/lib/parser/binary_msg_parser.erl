%%%-------------------------------------------------------------------
%%% @author heyoka
%%% @copyright (C) 2019, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 01. Aug 2019 16:53
%%%-------------------------------------------------------------------
-module(binary_msg_parser).
-author("heyoka").

-include("faxe.hrl").
%% API
-export([convert/3, convert/4]).

-callback parse(BinData :: binary()) -> map().

%% @doc parse and convert binary-data
-spec convert(Data :: binary(), binary(), Parser :: atom()) -> #data_point{}.
convert(Data, As, undefined) when is_binary(Data)->
   NewPoint = flowdata:set_field(#data_point{ts = faxe_time:now()}, As, Data),
   NewPoint;
convert(Data, As, Parser) when is_binary(Data)->
   convert(Data, #data_point{ts = faxe_time:now()}, As, Parser).
convert(Data, Point = #data_point{}, As, Parser) when is_binary(Data) ->
   {_T, NPoint, D} =
   case timer:tc(Parser, parse, [Data]) of
      {Time, {_DataFormat, _Vers, _Map}=D0} -> {Time, Point, D0};
      {Time, {Ts, DataFormat, Vers, Map}} -> {Time, Point#data_point{ts=Ts}, {DataFormat, Vers, Map}}
   end,
%%   lager:notice("Parser time <~p>: ~p",[Parser, T]),
   {DataF, Vs, Mp} = D,
   P0 = flowdata:set_field(NPoint, As, Mp),
   flowdata:set_field(P0, <<"df">>, DataF).

%%%%%%%
