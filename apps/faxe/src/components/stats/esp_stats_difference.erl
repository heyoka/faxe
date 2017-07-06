%% Date: 09.12.16 - 18:02
%% Ⓒ 2016 heyoka
-module(esp_stats_difference).
-author("Alexander Minichmair").

-inherit(esp_stats).
-behavior(esp_stats).
%% API
-export([execute/2, options/0]).

options() ->
   esp_stats:get_options() ++ [{module, atom, ?MODULE}].

execute({Tss, Values}, _Opts) ->
   Res = calc(lists:reverse(Values), lists:reverse(Tss), []),
   lists:unzip(Res).

calc([], [], Acc) ->
   Acc;
calc([_Val], [_Ts], Acc) ->
   Acc;
calc([H| TVal], [_HTs|TTs], Acc) ->
   [Sec | _] = TVal,
   [SecTs| _] = TTs,
   calc(TVal, TTs, [{SecTs, abs(H - Sec)}|Acc]).

-ifdef(TEST).
%%   basic_test() -> ?assertEqual([2,5,8,39], execute([1,3,8,16,55])).
-endif.