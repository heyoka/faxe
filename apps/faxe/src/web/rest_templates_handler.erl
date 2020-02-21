%%%-------------------------------------------------------------------
%%% @author $author
%%% @copyright (C) $year, $company
%%% @doc
%%%
%%% @end
%%% Created : $fulldate
%%%-------------------------------------------------------------------
-module(rest_templates_handler).

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


list_json(Req, State=#state{mode = _Mode}) ->
   #{orderby := OrderBy, dir := Direction} =
      cowboy_req:match_qs([{orderby, [], <<"changed">>}, {dir, [], <<"desc">>}], Req),
   L = lists:flatten(faxe:list_templates()),
   Sorted = lists:sort(order_fun(OrderBy, Direction), L),
   Maps = [rest_helper:template_to_map(T) || T <- Sorted],
   {jiffy:encode(Maps), Req, State}.


order_fun(<<"id">>, Dir) ->
   fun(#template{id = AId}, #template{id = BId}) -> (AId =< BId) == order_dir(Dir) end;
order_fun(<<"name">>, Dir) ->
   fun(#template{name = AId}, #template{name = BId}) -> (AId =< BId) == order_dir(Dir) end;
order_fun(_, Dir) ->
   fun(#template{date = AId}, #template{date = BId}) -> (AId =< BId) == order_dir(Dir) end.

order_dir(<<"asc">>) -> true;
order_dir(_) -> false.
