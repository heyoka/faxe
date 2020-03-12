%%%-------------------------------------------------------------------
%%% @author heyoka
%%% @copyright (C) 2019, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 01. Aug 2019 16:53
%%%-------------------------------------------------------------------
-module(parser_noop).
-author("heyoka").

-behavior(binary_msg_parser).

%% API
-export([parse/1]).


parse(BinData) ->
   BinData.