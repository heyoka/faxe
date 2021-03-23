%% Date: 23.03.21 - 18:02
%% Ⓒ 2021 heyoka
-module(esp_count_distinct).
-author("Alexander Minichmair").

-inherit(esp_stats).
-behavior(esp_stats).

-include("faxe.hrl").
%% API
-export([execute/2, options/0]).

options() ->
   esp_stats:get_options() ++ [{module, atom, ?MODULE}].


execute({Tss, Values}, _Opts) when is_list(Tss) ->
   Set = sets:from_list(Values),
   {first, sets:size(Set)}.

-ifdef(TEST).
   basic_test() ->
      ?assertEqual({first, 4}, execute({[1,2,3,4,5], [1,2,1,2,4,6]}, #{})).
   basic_2_test() ->
      ?assertEqual({first, 4}, execute({[1,2,3,4,5], [<<"1">>,<<"2">>,<<"1">>,<<"a">>,<<"b">>,<<"2">>]}, #{})).
-endif.
