%% Date: 12.05.17 - 20:58
%% Ⓒ 2017 LineMetrics GmbH
-module(c_noop).
-author("Alexander Minichmair").


-behavior(esp_stats).
%% API
-export([execute/2, options/0]).

options() ->
   esp_stats:get_options() ++ [{module, atom, ?MODULE}].

execute({_Tss, _Values} = Data, _S) ->
   Data.

