%% Date: 09.12.16 - 18:02
%% Ⓒ 2016 heyoka
-module(esp_bottom).
-author("Alexander Minichmair").

-include("faxe.hrl").

-inherit(esp_stats).
-behavior(esp_stats).
%% API
-export([execute/2, options/0]).

options() ->
   esp_stats:get_options() ++ [{num, integer, 1},{module, atom, ?MODULE}].

execute({Tss, Values}, #{num := Num}) when is_list(Values) ->
   lager:debug("execute with: ~p",[Values]),
   New = lists:zip(Tss, Values),
   Sorted = lists:usort( fun({_Ts, V1}, {_Ts2, V2}) -> V2 =< V1 end, New),
   Len = length(Sorted),
   RLen = case Len >= Num of true -> Len-Num; false -> 0 end,
   lists:unzip(lists:reverse(lists:nthtail(RLen, Sorted))).


-ifdef(TEST).

   basic_test() ->
      ?assertEqual(
         {[8,5,2,4,6],[319,322,326,328,331]},
         esp_bottom:execute({[1,2,3,4,5,6,7,8,9],[399,326,354,328,322,331,388,319,377]},#{num=>5})
      ),
      ?assertEqual(
         {[8,5,2,4,6,3,9,7,1],[319,322,326,328,331,354,377,388,399]},
         esp_bottom:execute({[1,2,3,4,5,6,7,8,9],[399,326,354,328,322,331,388,319,377]},#{num=>65})
      ).
-endif.