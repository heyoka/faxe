%% Date: 09.12.16 - 18:02
%% Ⓒ 2016 heyoka
-module(esp_avg).
-author("Alexander Minichmair").


-behavior(esp_stats).
%% API
-export([execute/2, options/0]).

options() ->
   esp_stats:get_options() ++ [{module, atom, ?MODULE}].

execute({_Tss, Values}, _S) ->
   {first, lists:sum(Values)/length(Values)}.

-ifdef(TEST).
%%   basic_test() -> ?assertEqual(16.6, execute([1,3,8,16,55], #{field => <<"val">>})).
-endif.

