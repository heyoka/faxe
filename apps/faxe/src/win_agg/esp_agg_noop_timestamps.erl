%% Date: 09.12.16 - 18:02
%% Ⓒ 2016 heyoka
%% simple window noop aggregation, which will emit all timestamps currently in the window
%% no calculations will be applied
-module(esp_agg_noop_timestamps).
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

emit(#esp_win_stats{} = Events, State) ->
   {ok, execute(Events), State}.

execute(#esp_win_stats{events = {Timestamps, _Values, _Events}}) ->
   lager:debug("execute with: ~p",[Timestamps]),
   Timestamps.
