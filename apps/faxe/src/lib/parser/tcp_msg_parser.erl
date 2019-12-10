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
   {T, {DataFormat, Vers, Map}} = timer:tc(Parser, parse, [Data]),
   lager:notice("Parser time <~p>: ~p",[Parser, T]),
   P0 = flowdata:set_field(Point, As, Map),
   P1 = flowdata:set_field(P0, <<"df">>, DataFormat),
   flowdata:set_field(P1, <<"vs">>, Vers).

%%%%%%%
