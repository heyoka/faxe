%%%-------------------------------------------------------------------
%%% @author $author
%%% @copyright (C) $year, $company
%%% @doc
%%%
%%% @end
%%% Created : $fulldate
%%%-------------------------------------------------------------------
-module(rest_tags_handler).

-include("faxe.hrl").

%%
%% Cowboy callbacks
-export([
  init/2
  , allowed_methods/2, list_json/2, content_types_provided/2, is_authorized/2]).

%%
%% Additional callbacks
-export([
]).

-record(state, {mode}).

init(Req, [{op, Mode}]) ->
   {cowboy_rest, Req, #state{mode = Mode}}.

is_authorized(Req, State) ->
  rest_helper:is_authorized(Req, State).

allowed_methods(Req, State) ->
    Value = [<<"GET">>],
    {Value, Req, State}.

content_types_provided(Req, State) ->
    {[
       {{<<"application">>, <<"json">>, []}, list_json}
    ], Req, State}.


list_json(Req, State=#state{mode = _Mode}) ->
   Tags = faxe:get_all_tags(),
   Map = #{<<"tags">> => Tags},
   {jiffy:encode(Map), Req, State}.

