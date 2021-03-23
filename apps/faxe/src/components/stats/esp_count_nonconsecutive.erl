%% Date: 23.03.21 - 18:02
%% Ⓒ 2021 heyoka
-module(esp_count_nonconsecutive).
-author("Alexander Minichmair").

-inherit(esp_stats).
-behavior(esp_stats).

-include("faxe.hrl").
%% API
-export([execute/2, options/0]).

options() ->
   esp_stats:get_options() ++ [{module, atom, ?MODULE}].

%% equality of values is: =:= (type and value)
execute({Tss, Values}, _Opts) when is_list(Tss) ->
   F = fun
          (Last, {Last, Count}) -> {Last, Count};
          (E, {_Last, Count}) -> {E, Count+1}
       end,
   {_L, Counter} = lists:foldl(F, {nil, 0}, Values),
   {first, Counter}.

-ifdef(TEST).
basic_test() ->
   ?assertEqual({first, 4}, execute({[1, 2, 3, 4, 5, 6], [1, 1, 1, 2, 1, 6]}, #{})).
basic_equality_test() ->
   ?assertEqual({first, 6}, execute({[1, 2, 3, 4, 5, 6], [1, 1.0, 1, 2, 1, 6]}, #{})).
basic_2_test() ->
   ?assertEqual({first, 5}, execute({[1, 2, 3, 4, 5, 6, 7],
      [<<"1">>, <<"2">>, <<"2">>, <<"a">>, <<"b">>, <<"b">>, <<"1">>]}, #{})).

-endif.
