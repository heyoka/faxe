%%%-------------------------------------------------------------------
%%% @author $author
%%% @copyright (C) $year, $company
%%% @doc
%%%
%%% @end
%%% Created : $fulldate
%%%-------------------------------------------------------------------
-module(rest_templates_handler).

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
   lager:notice("Cowboy Opts are : ~p",[Mode]),
   {cowboy_rest, Req, #state{mode = Mode}}.

allowed_methods(Req, State) ->
    Value = [<<"GET">>, <<"OPTIONS">>],
    {Value, Req, State}.

content_types_provided(Req, State) ->
    {[
       {{<<"application">>, <<"json">>, []}, list_json},
       {{<<"text">>, <<"html">>, []}, list_json}
    ], Req, State}.


list_json(Req, State=#state{mode = Mode}) ->
   L = lists:flatten(faxe:list_running_tasks()),
   Maps = [rest_helper:template_to_map(T) || T <- L],
   {jsx:encode(Maps), Req, State}.
