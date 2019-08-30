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
-export([convert/4]).

-callback parse(BinData :: binary()) -> map().

%% @doc parse and convert binary-data
-spec convert(Data :: binary(), binary(), Extract :: true|falxe, Parser :: atom()) -> #data_point{}.
convert(Data, As, false, undefined) ->
   NewPoint = flowdata:set_field(#data_point{ts = faxe_time:now()}, As, Data),
%%   lager:notice("[~p] new point: ~p",[?MODULE, NewPoint]),
   NewPoint;
convert(Data, _As, true, undefined) ->
   NewPoint = flowdata:extract_map(#data_point{ts = faxe_time:now()}, Data),
%%  NewPoint = flowdata:set_field(#data_point{ts = faxe_time:now()}, As, Data),
%%   lager:notice("[~p] new point: ~p",[?MODULE, NewPoint]),
   NewPoint;
convert(Data, As, Extract, Parser) ->
   {T, Map} = timer:tc(Parser, parse, [Data]),
%%   lager:notice("[~p] parser time: ~p",[?MODULE, T]),
   convert(Map, As, Extract, undefined).