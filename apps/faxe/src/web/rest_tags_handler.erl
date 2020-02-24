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
   , allowed_methods/2, list_json/2, content_types_provided/2]).

%%
%% Additional callbacks
-export([
]).

-record(state, {mode}).

init(Req, [{op, Mode}]) ->
   {cowboy_rest, Req, #state{mode = Mode}}.

allowed_methods(Req, State) ->
    Value = [<<"GET">>, <<"OPTIONS">>],
    {Value, Req, State}.

content_types_provided(Req, State) ->
    {[
       {{<<"application">>, <<"json">>, []}, list_json}
    ], Req, State}.


list_json(Req, State=#state{mode = _Mode}) ->
   Tags = faxe:get_all_tags(),
   Map = #{<<"tags">> => Tags},
   {jiffy:encode(Map), Req, State}.

