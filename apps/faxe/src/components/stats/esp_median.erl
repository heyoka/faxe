%% Date: 09.12.16 - 18:02
%% Ⓒ 2016 heyoka
-module(esp_median).
-author("Alexander Minichmair").

-inherit(esp_stats).
-behavior(esp_stats).
%% API
-export([execute/2, options/0]).

options() ->
   esp_stats:get_options() ++ [{module, atom, ?MODULE}].

execute({_Tss, Values}, _Opts) ->
   Length = length(Values),
   Sorted = lists:sort(Values),
   M =
   case (Length rem 2) == 0 of
      false ->
         lists:nth(trunc((Length+1)/2), Sorted);
      true ->
         Nth = trunc(Length/2),
         (lists:nth(Nth, Sorted) + lists:nth(Nth+1, Sorted)) / 2
   end,
   {first, M}.
