%% Date: 09.12.16 - 18:02
%% Ⓒ 2016 heyoka
-module(esp_stats_derivative).
-author("Alexander Minichmair").

-include("faxe.hrl").
%% API
-export([execute/1]).

%% todo implement for real -> diff/(timediff/timeunit)
execute(#esp_win_stats{events = {_Timestamps, Values, _Events}}) ->
   lager:debug("execute with: ~p",[Values]),
   calc(Values, []).

calc([], Acc) ->
   Acc;
calc([_Val], Acc) ->
   Acc;
calc([H|T], Acc) ->
   [Sec | _] = T,
   calc(T, Acc ++ [abs(H - Sec)]).