%% Date: 09.12.16 - 18:02
%% Ⓒ 2016 heyoka
-module(esp_sum).
-author("Alexander Minichmair").


-behavior(esp_stats).
%% API
-export([execute/2, options/0]).

options() ->
   esp_stats:get_options() ++ [{module, atom, ?MODULE}].

execute({_Tss, Values}, _Opts) ->
   lager:debug("execute with: ~p",[Values]),
   {first, lists:sum(Values)}.


-ifdef(TEST).

%%basic_test() ->
%%   ?assertEqual(22, execute([10,2,2,2,6])).

-endif.