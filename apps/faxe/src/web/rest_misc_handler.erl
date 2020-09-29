%%%-------------------------------------------------------------------
%%% @author $author
%%% @copyright (C) $year, $company
%%% @doc
%%%
%%% @end
%%% Created : $fulldate
%%%-------------------------------------------------------------------
-module(rest_misc_handler).

%%
%% Cowboy callbacks
-export([
   init/2, allowed_methods/2, config_json/2, content_types_provided/2]).

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
       {{<<"application">>, <<"json">>, []}, config_json}
    ], Req, State}.


%% faxe's config
config_json(Req, State=#state{mode = config}) ->
  AllConf = application:get_all_env(faxe),
  lager:notice("AccConfig: ~p", [AllConf]),
   Stats = faxe_vmstats:called(),
   F = fun(K, V, Acc) ->
      NewKey = binary:replace(list_to_binary(K), <<".">>, <<"-">>, []),
      Acc#{NewKey => V}
      end,
   Map = maps:fold(F, #{}, Stats),
   {jiffy:encode(Map), Req, State}.
