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

suite() ->
   [{timetrap, {minutes, 2}}].


all() ->
   [].


%%filter_nochange_test() ->
%%   Point = #data_point{ts = 1, fields = [{<<"val">>, 1}, {<<"val1">>, 1.343}, {<<"val2">>, 2.222}]},
%%   LastVals = [],
%%   ?assertEqual(Point#data_point.fields, filter(Point, LastVals)).
%%filter_normal_test() ->
%%   Point = #data_point{ts = 1, fields = [{<<"val">>, 1}, {<<"val1">>, 1.343}, {<<"val2">>, 2.222}]},
%%   LastVals = [{<<"val1">>, 1.343}, {<<"val2">>, 2.222}],
%%   ?assertEqual([{<<"val">>, 1}], filter(Point, LastVals)).
%%filter_other_test() ->
%%   Point = #data_point{ts = 1, fields = [{<<"val">>, 1}, {<<"val1">>, 1.343}, {<<"val2">>, 2.222}]},
%%   LastVals = [{<<"val5">>, 1.343}, {<<"val2">>, 2.222}],
%%   ?assertEqual([{<<"val">>, 1}, {<<"val1">>, 1.343}], filter(Point, LastVals)).
%%filter_all_test() ->
%%   Point = #data_point{ts = 1, fields = [{<<"val">>, 1}, {<<"val1">>, 1.343}, {<<"val2">>, 2.222}]},
%%   LastVals = [{<<"val">>, 1}, {<<"val1">>, 1.343}, {<<"val2">>, 2.222}],
%%   ?assertEqual([], filter(Point, LastVals)).
%%filter_equality_test() ->
%%   Point = #data_point{ts = 1, fields = [{<<"val">>, 1.0}, {<<"val1">>, 1.343}, {<<"val2">>, 2.222}]},
%%   LastVals = [{<<"val">>, 1}, {<<"val1">>, 1.343}, {<<"val2">>, 2.222}],
%%   ?assertEqual([{<<"val">>, 1.0}], filter(Point, LastVals)).
%%process_test() ->
%%   Point = #data_point{ts = 1, fields = [{<<"val">>, 1.0}, {<<"val1">>, 1.343}, {<<"val2">>, 2.222}]},
%%   LastVals = [{<<"val">>, 1}, {<<"val2">>, 2.222}],
%%   State = #state{reset_timeout = undefined, fields = [<<"val">>,<<"val2">>], values = LastVals},
%%   ?assertEqual(process(1, Point, State),
%%      {emit, Point#data_point{fields = [{<<"val">>, 1.0}, {<<"val1">>, 1.343}] },
%%         State#state{values = [{<<"val">>, 1.0}, {<<"val2">>, 2.222}]}}
%%   ).