%% Date: 09.12.16 - 18:02
%% Ⓒ 2016 heyoka
-module(esp_first).
-author("Alexander Minichmair").


-behavior(esp_stats).
%% API
-export([execute/2, options/0]).

options() ->
   esp_stats:get_options() ++ [{module, atom, ?MODULE}].

execute({_Tss, Values}, _Opts) ->
   {first, lists:last(Values)}.