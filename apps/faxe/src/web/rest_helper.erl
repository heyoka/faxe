%%%-------------------------------------------------------------------
%%% @author heyoka
%%% @copyright (C) 2019, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 20. Aug 2019 16:02
%%%-------------------------------------------------------------------
-module(rest_helper).
-author("heyoka").

-include("faxe.hrl").

%% API
-export([task_to_map/1, template_to_json/1]).



%%id :: list()|binary(),
%%name :: binary(),
%%definition :: map(),
%%date :: faxe_time:date(),
%%pid :: pid(),
%%last_start :: faxe_time:date(),
%%last_stop :: faxe_time:date()
task_to_map(T = #task{definition = Def0, id = Id, name = Name,
   date = Dt, last_start = LStart, last_stop = LStop}) ->
   lager:notice("task_to_json: ~p",[T]),
%%   Fields = record_info(fields, T),
%%   Def = maps:from_list(Def0),
   Map = #{id => Id, name => Name,
      created => faxe_time:to_iso8601(Dt),
      last_start => faxe_time:to_iso8601(LStart),
      last_stop => faxe_time:to_iso8601(LStop)},
   lager:notice("theMap: ~p",[Map]),
   Map.

template_to_json(T = #template{definition = Def0, id = Id, name = Name, date = Dt, dfs = Dfs}) ->
   lager:notice("template_to_json: ~p",[T]),
%%   Fields = record_info(fields, T),
%%   Def = maps:from_list(Def0),
   Map = #{id => Id, name => Name, dfs => Dfs, date => faxe_time:to_iso8601(Dt)},
   lager:notice("theTMap: ~p",[Map]),
   Map.

%%record_to_map(Name, Record) ->
%%   lists:foldl(
%%      fun({I, E}, Acc) -> Acc#{E => element(I, Record) } end,
%%      #{},
%%      lists:zip(lists:seq(2, (record_info(size, Name))), (record_info(fields, Name)))
%%   ).

