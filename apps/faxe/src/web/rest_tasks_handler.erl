%%%-------------------------------------------------------------------
%%% @author $author
%%% @copyright (C) $year, $company
%%% @doc
%%%
%%% @end
%%% Created : $fulldate
%%%-------------------------------------------------------------------
-module(rest_tasks_handler).

-include("faxe.hrl").

%%
%% Cowboy callbacks
-export([
   init/2
   , allowed_methods/2, list_json/2, content_types_provided/2]).

%%
%% Additional callbacks
-export([
]).

-record(state, {mode}).

init(Req, [{op, Mode}]) ->
%%   lager:notice("Cowboy Opts are : ~p",[Mode]),
   {cowboy_rest, Req, #state{mode = Mode}}.

allowed_methods(Req, State) ->
    Value = [<<"GET">>, <<"OPTIONS">>],
    {Value, Req, State}.

content_types_provided(Req, State) ->
    {[
       {{<<"application">>, <<"json">>, []}, list_json}
    ], Req, State}.


list_json(Req, State=#state{mode = Mode}) ->
   Tasks =
      case Mode of
         list -> faxe:list_tasks();
         list_running -> faxe:list_running_tasks();
         list_by_template ->
            TId = cowboy_req:binding(template_id, Req),
            faxe:list_tasks_by_template(binary_to_integer(TId))
          end,
   Fun = fun(#task{date = AId}, #task{date = BId}) -> (AId =< BId) == false end,
   Sorted = lists:sort(Fun, Tasks),
   Maps = [rest_helper:task_to_map(T) || T <- Sorted],
   {jiffy:encode(Maps), Req, State}.
