%% Date: 09.12.16 - 18:02
%% Ⓒ 2016 heyoka
-module(esp_variance).
-author("Alexander Minichmair").



-behavior(esp_stats).
%% API
-export([execute/2, options/0]).

options() ->
   esp_stats:get_options() ++ [{num, integer, 1}, {module, atom, ?MODULE}].

execute({_Tss, Values}, _Opts) ->
   lager:debug("execute with: ~p",[Values]),
   {first, mathex:variance(Values)}.
