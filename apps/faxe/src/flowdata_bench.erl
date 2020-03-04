%%%-------------------------------------------------------------------
%%% @author heyoka
%%% @copyright (C) 2020, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 02. Mar 2020 10:52
%%%-------------------------------------------------------------------
-module(flowdata_bench).
-author("heyoka").
-include("faxe.hrl").
%% API
-export([do/0, do/1, normal/1, zipped/1, zipped_prepath/1, normal_get/1,
   zipped_get/1, zipped_prepath_get/1, set_fields_flowdata/1, set_fields_native/1, set/0, set/1]).


do() -> do(1000).
do(N) ->

   {TN, _} = timer:tc(?MODULE, zipped_prepath, [N]),
   lager:notice("zipped_prepath with ~p runs: ~p ms",[N, round(TN/1000)]),
   {TN1, _} = timer:tc(?MODULE, zipped, [N]),
   lager:notice("zipped with ~p runs: ~p ms",[N, round(TN1/1000)]),
   {TN0, _} = timer:tc(?MODULE, normal, [N]),
   lager:notice("normal with ~p runs: ~p ms",[N, round(TN0/1000)]),
   {TN01, _} = timer:tc(?MODULE, normal_get, [N]),
   lager:notice("normal_get with ~p runs: ~p ms",[N, round(TN01/1000)]),

   {TN10, _} = timer:tc(?MODULE, zipped_get, [N]),
   lager:notice("zipped_get with ~p runs: ~p ms",[N, round(TN10/1000)]),
   {TN010, _} = timer:tc(?MODULE, zipped_prepath_get, [N]),
   lager:notice("zipped_prepath_get with ~p runs: ~p ms",[N, round(TN010/1000)]).


set() -> set(1000).
set(N) ->
   {TN, _} = timer:tc(?MODULE, set_fields_native, [N]),
   lager:notice("set_native ~p runs: ~p ms",[N, round(TN/1000)]),
   {TN1, _} = timer:tc(?MODULE, set_fields_flowdata, [N]),
   lager:notice("set_flowdata ~p runs: ~p ms",[N, round(TN1/1000)]).

normal(N) ->
   Keys = [<<"my.deep.path">>, <<"anotherpath">>, <<"a.third.path.here">>],
   Vals = [<<"myvalue1">>, <<"myvalue1">>,<<"myvalue1">>],
   lists:foldl(
      fun(_E, P) ->
         flowdata:set_fields(P, Keys, Vals) end,
      #data_point{ts = 13},
      lists:seq(0,N)
   ).

normal_get(N) ->
   Keys = [<<"my.deep.path">>, <<"anotherpath">>, <<"a.third.path.here">>],
   Vals = [<<"myvalue1">>, <<"myvalue1">>,<<"myvalue1">>],
   lists:foldl(
      fun(_E, P) ->
         [flowdata:fields(P, Keys)],
         flowdata:set_fields(P, Keys, Vals) end,
      #data_point{ts = 13},
      lists:seq(0,N)
   ).

zipped(N) ->
   Keys = [<<"my.deep.path">>, <<"anotherpath">>, <<"a.third.path.here">>],
   Vals = [<<"myvalue1">>, <<"myvalue1">>,<<"myvalue1">>],
   KeyVal = lists:zip(Keys, Vals),
   lists:foldl(
      fun(_E, P) ->
         flowdata:set_fields(P, KeyVal) end,
      #data_point{ts = 13},
      lists:seq(0,N)
   ).

zipped_get(N) ->
   Keys = [<<"my.deep.path">>, <<"anotherpath">>, <<"a.third.path.here">>],
   Vals = [<<"myvalue1">>, <<"myvalue1">>,<<"myvalue1">>],
   KeyVal = lists:zip(Keys, Vals),
   lists:foldl(
      fun(_E, P) ->
         [flowdata:fields(P, Keys)],
         flowdata:set_fields(P, KeyVal) end,
      #data_point{ts = 13},
      lists:seq(0,N)
   ).

zipped_prepath(N) ->
   Keys = [flowdata:path(<<"my.deep.path">>), flowdata:path(<<"anotherpath">>), flowdata:path(<<"a.third.path.here">>)],
   Vals = [<<"myvalue1">>, <<"myvalue1">>,<<"myvalue1">>],
   KeyVal = lists:zip(Keys, Vals),
   lists:foldl(
      fun(_E, P) ->
         flowdata:set_fields(P, KeyVal) end,
      #data_point{ts = 13},
      lists:seq(0,N)
   ).

zipped_prepath_get(N) ->
   Keys = [flowdata:path(<<"my.deep.path">>), flowdata:path(<<"anotherpath">>), flowdata:path(<<"a.third.path.here">>)],
   Vals = [<<"myvalue1">>, <<"myvalue1">>,<<"myvalue1">>],
   KeyVal = lists:zip(Keys, Vals),
   lists:foldl(
      fun(_E, P) ->
         [flowdata:fields(P, Keys)],
         flowdata:set_fields(P, KeyVal) end,
      #data_point{ts = 13},
      lists:seq(0,N)
   ).


set_fields_native(N) ->
   Keys = [flowdata:path(<<"my">>), flowdata:path(<<"anotherpath">>), flowdata:path(<<"third_path">>)],
   Vals = [<<"myvalue1">>, <<"myvalue2">>,<<"myvalue3">>],
   KeyVal = lists:zip(Keys, Vals),
   lists:foldl(
      fun(_E, P=#data_point{fields = Fields}) ->
         P#data_point{fields = maps:merge(Fields, maps:from_list(KeyVal))}
      end,
      #data_point{ts = 13},
      lists:seq(0,N)
   ).

set_fields_flowdata(N) ->
   Keys = [flowdata:path(<<"my">>), flowdata:path(<<"anotherpath">>), flowdata:path(<<"third_path">>)],
   Vals = [<<"myvalue1">>, <<"myvalue2">>,<<"myvalue3">>],
   KeyVal = lists:zip(Keys, Vals),
   lists:foldl(
      fun(_E, P) ->
         flowdata:set_fields(P, KeyVal) end,
      #data_point{ts = 13},
      lists:seq(0,N)
   ).

