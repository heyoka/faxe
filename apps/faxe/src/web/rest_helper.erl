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

task_to_map(_T = #task{
   id = Id, name = Name, date = Dt, is_running = Running,
   last_start = LStart, last_stop = LStop, dfs = Dfs, permanent = Perm,
   template = Template, template_vars = TemplateVars, tags = Tags
}) ->
   Map = #{
      <<"id">> => Id,
      <<"name">> => Name,
      <<"dfs">> => Dfs,
      <<"running">> => Running,
      <<"permanent">> => Perm,
      <<"changed">> => faxe_time:to_iso8601(Dt),
      <<"last_start">> => faxe_time:to_iso8601(LStart),
      <<"last_stop">> => faxe_time:to_iso8601(LStop),
      <<"tags">> => Tags
   },
   OutMap =
   case Template of
      <<>> -> Map;
      _ -> Map#{<<"template">> => Template, <<"template_vars">> => TemplateVars}
   end,
   OutMap.

template_to_map(_T = #template{definition = _Def0, id = Id, name = Name, date = Dt, dfs = Dfs}) ->
   Map = #{
      <<"id">> => Id,
      <<"name">> => Name,
      <<"dfs">> => Dfs,
      <<"changed">> => faxe_time:to_iso8601(Dt)},
   Map.


do_register(Req, State, Type) ->
   {ok, Result, Req3} = cowboy_req:read_urlencoded_body(Req),
   TaskName = proplists:get_value(<<"name">>, Result),
   Dfs = proplists:get_value(<<"dfs">>, Result),
   Tags = proplists:get_value(<<"tags">>, Result, []),
%%   lager:notice("name: ~p: dfs: ~p, type:~p",[TaskName, Dfs, Type]),
   case reg_fun(Dfs, TaskName, Type) of
      ok ->
         Id = get_task_or_template_id(TaskName, Type),
         case Type of
            task -> case Tags of
                       [] -> ok;
                       TagJson -> faxe_db:add_tags(Id,jiffy:decode(TagJson))
                    end
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

get_task_or_template_id(TName, task) ->
   NewTask = faxe:get_task(TName),
      case NewTask of
         #task{id = NewId} -> NewId;
         _  -> 0
      end;
get_task_or_template_id(TName, _) ->
   NewTask = faxe:get_template(TName),
      case NewTask of
         #template{id = NewId} -> NewId;
         _  -> 0
      end.

-spec to_bin(any()) -> binary().
to_bin(L) when is_list(L) -> list_to_binary(L);
to_bin(E) when is_atom(E) -> atom_to_binary(E, utf8);
to_bin(Bin) when is_binary(Bin) -> Bin;
to_bin(Bin) -> lager:warning("to bin: ~p",[Bin]), Bin.