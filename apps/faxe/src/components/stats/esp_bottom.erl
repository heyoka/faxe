%% Date: 09.12.16 - 18:02
%% Ⓒ 2016 heyoka
-module(esp_bottom).
-author("Alexander Minichmair").

-inherit(esp_stats).
-behavior(esp_stats).
%% API
-export([execute/2, options/0]).

options() ->
   esp_stats:get_options() ++ [{num, integer, 1},{module, atom, ?MODULE}].

execute({Tss, Values}, #{num := Num}) when is_list(Values) ->
   lager:debug("execute with: ~p",[Values]),
   Sorted = lists:sort(Values),
   Len = length(Sorted),
   {lists:nthtail(Len-Num, Tss), lists:nthtail(Len-Num, Sorted)}.