%% Date: 12.05.17 - 20:58
%% Ⓒ 2017 LineMetrics GmbH
-module(c_noop).
-author("Alexander Minichmair").

-inherit(esp_stats).
-behavior(esp_stats).
%% API
-export([execute/2, options/0]).

options() ->
   get_options() ++ [{module, atom, ?MODULE}].

execute({_Tss, _Values} = Data, _S) ->
   Data.

