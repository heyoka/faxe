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
-export([convert/3]).

-callback parse(BinData :: binary()) -> map().

%% @doc parse and convert binary-data
-spec convert(Data :: binary(), binary(), Parser :: atom()) -> #data_point{}.
convert(Data, As, undefined) ->
   NewPoint = flowdata:set_field(#data_point{ts = faxe_time:now()}, As, Data),
   NewPoint;
convert(Data, As, Parser) ->
   {T, {DataFormat, Vers, Map}} = timer:tc(Parser, parse, [Data]),
   NewPoint = flowdata:set_field(#data_point{ts = faxe_time:now()}, As, Map),
   NewPoint1 = flowdata:set_field(NewPoint, <<"df">>, DataFormat),
   lager:notice("[~p] parser time: ~p",[?MODULE, T]),
   flowdata:set_field(NewPoint1, <<"vs">>, Vers).

%%%%%%%
