%% Date: 09.12.16 - 18:02
%% Ⓒ 2016 heyoka
-module(esp_top).
-author("Alexander Minichmair").

-include("faxe.hrl").

-inherit(esp_stats).
-behavior(esp_stats).
%% API
-export([execute/2, options/0, basic_test/0]).

options() ->
   esp_stats:get_options() ++ [{num, integer, 1}, {module, atom, ?MODULE}].

%%% @todo look for mixed up timestamps after sorting the value-list !!
execute({Tss, Values}, #{num := Num}) when is_list(Values) ->
   lager:debug("execute with: ~p",[Values]),
   New = lists:zip(Tss, Values),
   Sorted = lists:usort( fun({_Ts, V1}, {_Ts2, V2}) -> V1 =< V2 end, New),
   Len = length(Sorted),
   RLen = case Len >= Num of true -> Len-Num; false -> 0 end,
   lists:unzip(lists:reverse(lists:nthtail(RLen, Sorted))).


-ifdef(TEST).
basic_test() -> ?assertEqual(
   {[1,7,9,3],[399,388,377,354]},
   execute({[1,2,3,4,5,6,7,8,9],[399,326,354,328,322,331,388,319,377]},#{num=>4})
).
-endif.



