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
-export([task_to_map/1, template_to_map/1, do_register/3, to_bin/1, reg_fun/3]).


task_to_map(_T = #task{id = Id, name = Name, date = Dt, is_running = Running,
   last_start = LStart, last_stop = LStop, dfs = Dfs, permanent = Perm}) ->
%%   lager:notice("task_to_json: ~p",[T]),
   Map = #{id => Id, name => Name,
      dfs => Dfs,
      running => Running,
      permanent => Perm,
      created => faxe_time:to_iso8601(Dt),
      last_start => faxe_time:to_iso8601(LStart),
      last_stop => faxe_time:to_iso8601(LStop)},
%%   lager:notice("theMap: ~p",[Map]),
   Map.

template_to_map(_T = #template{definition = _Def0, id = Id, name = Name, date = Dt, dfs = Dfs}) ->
%%   lager:notice("template_to_json: ~p",[T]),
   Map = #{id => Id, name => Name, dfs => to_bin(Dfs), date => faxe_time:to_iso8601(Dt)},
%%   lager:notice("theTMap: ~p",[Map]),
   Map.


do_register(Req, State, Type) ->
   {ok, Result, Req3} = cowboy_req:read_urlencoded_body(Req),
   TaskName = proplists:get_value(<<"name">>, Result),
   Dfs = proplists:get_value(<<"dfs">>, Result),
%%   lager:notice("name: ~p: dfs: ~p, type:~p",[TaskName, Dfs, Type]),
   case reg_fun(Dfs, TaskName, Type) of
      ok ->
         NewTask = faxe:get_task(TaskName),
         Id =
         case NewTask of
            #task{id = NewId} -> NewId;
            _  -> 0
         end,
         Req4 = cowboy_req:set_resp_body(
            jiffy:encode(#{success => true, name => TaskName, id => Id}), Req3),
         {true, Req4, State};
      {error, Error} ->
         Add =
            case Type of
               task -> "";
               _ -> "-template"
            end,
         lager:warning("Error occured when registering faxe-flow"++Add++": ~p",[Error]),
         Req4 = cowboy_req:set_resp_body(jiffy:encode(#{success => false, error => to_bin(Error)}), Req3),
         {false, Req4, State}
   end.

reg_fun(Dfs, Name, task) -> Res = faxe:register_string_task(Dfs, Name), Res;
reg_fun(Dfs, Name, _) -> faxe:register_template_string(Dfs, Name).

-spec to_bin(any()) -> binary().
to_bin(L) when is_list(L) -> list_to_binary(L);
to_bin(E) when is_atom(E) -> atom_to_binary(E, utf8);
to_bin(Bin) when is_binary(Bin) -> Bin;
to_bin(Bin) -> lager:warning("to bin: ~p",[Bin]), Bin.