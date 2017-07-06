%% Date: 09.12.16 - 18:02
%% Ⓒ 2016 heyoka
-module(esp_elapsed).
-author("Alexander Minichmair").

-inherit(esp_stats).
-behavior(esp_stats).
%% API
-export([execute/2, options/0]).

options() ->
   get_options() ++ [{module, atom, ?MODULE}].

execute({Tss, _Values}, _Opts) ->
   calc(Tss, []).

calc([], Acc) ->
   Acc;
calc([_Ts], Acc) ->
   Acc;
calc([H|T], Acc) ->
   [Sec | _] = T,
   calc(T, [{Sec, abs(H - Sec)}|Acc]).

-ifdef(TEST).
   basic_test() -> ?assertEqual([2,5,8,39], execute({[1,3,8,16,55], []}, #{})).
-endif.