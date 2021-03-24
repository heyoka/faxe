%%%-------------------------------------------------------------------
%%% @author heyoka
%%% @copyright (C) 2019, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 09. Sep 2019 13:50
%%%-------------------------------------------------------------------
-module(faxe_lambdalib_tests).
-author("heyoka").

%% API
-include("faxe.hrl").
-ifdef(TEST).
-compile(nowarn_export_all).
-compile([export_all]).
-include_lib("eunit/include/eunit.hrl").

round_float_test() ->
  ?assertEqual(125.456, faxe_lambda_lib:round_float(125.455679, 3)).

max_test() ->
  ?assertEqual(12, faxe_lambda_lib:max([1,12,3,6.8])).

min_test() ->
  ?assertEqual(1, faxe_lambda_lib:min([1,12,3,6.8])).

%% lists
is_member_list_test() ->
  ?assertEqual(true, faxe_lambda_lib:member(23, test_list())).
not_is_member_list_test() ->
  ?assertEqual(false, faxe_lambda_lib:member(23.0, test_list())).
is_notmember_list_test() ->
  ?assertEqual(true, faxe_lambda_lib:not_member(22, test_list())).
not_is_notmember_list_test() ->
  ?assertEqual(false, faxe_lambda_lib:not_member(8, test_list())).

test_list() -> [1,23,44.43,8].
%% maps
is_member_map_test() ->
  ?assertEqual(true, faxe_lambda_lib:member(<<"k2">>, test_map())).
not_is_member_map_test() ->
  ?assertEqual(false, faxe_lambda_lib:member(<<"k3">>, test_map())).
is_notmember_map_test() ->
  ?assertEqual(true, faxe_lambda_lib:not_member(<<"k3">>, test_map())).
not_is_notmember_map_test() ->
  ?assertEqual(false, faxe_lambda_lib:not_member(<<"k2">>, test_map())).

map_get_test() ->
  ?assertEqual(<<"yes">>, faxe_lambda_lib:map_get(<<"k2">>, test_map())).
map_get_undefined_test() ->
  ?assertEqual(undefined, faxe_lambda_lib:map_get(<<"k8">>, test_map())).

test_map() -> #{<<"k1">> => 2134.23, <<"k2">> => <<"yes">>, <<"four">> => 5}.
-endif.
