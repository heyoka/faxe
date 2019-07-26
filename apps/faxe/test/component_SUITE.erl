%%%-------------------------------------------------------------------
%%% @author heyoka
%%% @copyright (C) 2019, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 15. Jul 2019 15:06
%%%-------------------------------------------------------------------
-module(component_SUITE).
-author("heyoka").

-include_lib("common_test/include/ct.hrl").
%% API
-export([suite/0, all/0]).
%% tests
-export([test1/1, test2/1]).

suite() ->
   [{timetrap, {minutes, 2}}].


all() ->
   [test1, test2].

test1(_Config) ->
   1 = 1.

test2(_Config) ->
   A = 0,
   1/A.


