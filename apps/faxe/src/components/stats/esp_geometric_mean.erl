%% Date: 09.12.16 - 18:02
%% Ⓒ 2016 heyoka
-module(esp_geometric_mean).
-author("Alexander Minichmair").


-behavior(esp_stats).
%% API
-export([execute/2, options/0]).

options() ->
   esp_stats:get_options() ++ [{module, atom, ?MODULE}].

execute({_Tss, Values}, _Opts) when is_list(Values) ->
   Length = length(Values),
   [First|Values1] = Values,
   Prod = lists:foldl(
      fun(E, Acc) -> E * Acc end,
      First, Values1
   ),
   {first, mathex:nth_root(Length, Prod)}.

