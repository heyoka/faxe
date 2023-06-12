%% Date: 09.12.16 - 18:02
%% Ⓒ 2016 heyoka
-module(esp_percentile).
-author("Alexander Minichmair").


-behavior(esp_stats).
%% API
-export([execute/2, options/0]).

options() ->
   esp_stats:get_options() ++ [{perc, integer, 95}, {module, atom, ?MODULE}].

execute({_T, Values}, #{perc := Perc}) ->
   lager:debug("execute with: ~p",[Values]),
   {first, mathex:percentile(Values, Perc)}.
