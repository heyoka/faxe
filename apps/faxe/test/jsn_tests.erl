%%%-------------------------------------------------------------------
%%% @author heyoka
%%% @copyright (C) 2019, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 09. Sep 2019 13:50
%%%-------------------------------------------------------------------
-module(jsn_tests).
-author("heyoka").

%% API
-include("faxe.hrl").
-ifdef(TEST).
-compile(nowarn_export_all).
-compile([export_all]).
-include_lib("eunit/include/eunit.hrl").

jsn_set_basic_test() ->
   M1 = #{<<"data">> => #{<<"bar">> =>[#{<<"first">> => 1}]}},
   M2 = #{<<"second">> => 2},
   ?assertEqual(
      #{<<"data">> => #{<<"bar">> =>[#{<<"first">> => 1}, #{<<"second">> => 2}]}},
      jsn:set(flowdata:path(<<"data.bar[2]">>), M1, M2)
   ).
-endif.
