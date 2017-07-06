%% Date: 09.12.16 - 18:02
%% Ⓒ 2016 heyoka
-module(esp_top).
-author("Alexander Minichmair").


-inherit(esp_stats).
-behavior(esp_stats).
%% API
-export([execute/2, options/0]).

options() ->
   esp_stats:get_options() ++ [{num, integer, 1}, {module, atom, ?MODULE}].

execute({Tss, Values}, #{num := Num}) when is_list(Values) ->
   lager:debug("execute with: ~p",[Values]),
   Sorted = lists:usort( fun(P1, P2) -> P1 =< P2 end, Values),
   Len = length(Sorted),
   {lists:nthtail(Len-Num, Tss), lists:nthtail(Len-Num, Sorted)}.


