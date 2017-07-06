%% Date: 09.12.16 - 18:02
%% Ⓒ 2016 heyoka
-module(esp_max).
-author("Alexander Minichmair").

-inherit(esp_stats).
-behavior(esp_stats).
%% API
-export([execute/2, options/0]).

options() ->
   esp_stats:get_options() ++ [{module, atom, ?MODULE}].

execute({_T,Values}, _O) ->
   lager:debug("execute with: ~p",[Values]),
   {first, lists:max(Values)}.
