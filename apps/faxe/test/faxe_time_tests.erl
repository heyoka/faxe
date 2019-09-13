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
-endif.
