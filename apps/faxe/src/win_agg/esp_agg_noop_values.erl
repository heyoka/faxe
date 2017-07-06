%% Date: 09.12.16 - 18:02
%% Ⓒ 2016 heyoka
%% simple window noop aggregation, which will emit all values currently in the window
%% no calculations will be applied
-module(esp_agg_noop_values).
-author("Alexander Minichmair").

-include("faxe.hrl").

-behavior(esp_aggregate).
%% API
-export([execute/1, init/1, accumulate/2, compensate/2, emit/2]).

init(_) ->
   {ok, ok}.

accumulate(_E, State) ->
   {ok, State}.

compensate(_E, State) ->
   {ok, State}.

emit(#esp_win_stats{events = {_T, Values, _E}}, State) ->
   {ok, execute(Values), State}.

execute(Values) ->
   Values.
