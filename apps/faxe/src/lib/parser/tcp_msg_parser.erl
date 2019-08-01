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

%% API
-export([]).

-callback parse(BinData :: binary()) -> jsx:json_text().