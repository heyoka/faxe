%%%-------------------------------------------------------------------
%%% @author heyoka
%%% @copyright (C) 2019, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 09. Sep 2019 13:50
%%%-------------------------------------------------------------------
-module(faxe_time_tests).
-author("heyoka").

%% API
-include("faxe.hrl").
-ifdef(TEST).
-compile(nowarn_export_all).
-compile([export_all]).
-include_lib("eunit/include/eunit.hrl").

duration_to_ms_basic_test() ->
   Dur = <<"3h">>,
   ?assertEqual(3*60*60*1000, faxe_time:duration_to_ms(Dur)).

duration_to_ms_neg_test() ->
   Dur = <<"-3h">>,
   ?assertEqual(3*60*60*1000*-1, faxe_time:duration_to_ms(Dur)).

duration_zero_test() ->
   Dur = <<"0m">>,
   ?assertEqual(0, faxe_time:duration_to_ms(Dur)).

duration_big_test() ->
   Dur = <<"70000ms">>,
   ?assertEqual(70*1000, faxe_time:duration_to_ms(Dur)).

not_valid_duration_test() ->
   Dur = <<"25mm">>,
   ?assertError(function_clause, faxe_time:duration_to_ms(Dur)).

valid_duration_test() ->
   Dur = <<"1255ms">>,
   ?assertEqual(true, faxe_time:is_duration_string(Dur)).

not_valid_duration_2_test() ->
   Dur = <<"1255ds">>,
   ?assertEqual(false, faxe_time:is_duration_string(Dur)).

-endif.
