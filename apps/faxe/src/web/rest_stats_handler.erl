%%%-------------------------------------------------------------------
%%% @author $author
%%% @copyright (C) $year, $company
%%% @doc
%%%
%%% @end
%%% Created : $fulldate
%%%-------------------------------------------------------------------
-module(rest_stats_handler).

%%
%% Cowboy callbacks
-export([
  init/2, allowed_methods/2, stats_json/2, content_types_provided/2, is_authorized/2]).

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
       {{<<"application">>, <<"json">>, []}, stats_json}
    ], Req, State}.


%% VM STATS
stats_json(Req, State=#state{mode = vm}) ->
   Stats = faxe_vmstats:called(),
   F = fun(K, V, Acc) ->
      NewKey = binary:replace(list_to_binary(K), <<".">>, <<"-">>, []),
      Acc#{NewKey => V}
      end,
   Map = maps:fold(F, #{}, Stats),
   {jiffy:encode(Map), Req, State};

%% FAXE STATS
stats_json(Req, State=#state{mode = faxe}) ->
   Stats = faxe_stats:get_stats(),
   {jiffy:encode(Stats), Req, State};

%% Processes by reductions STATS
stats_json(Req, State=#state{mode = reds}) ->
  Stats = process_stats:get_top_reds(20),
  {jiffy:encode(Stats), Req, State};

%% Graph node processes by message-q size STATS
stats_json(Req, State=#state{mode = msgq}) ->
  Stats = process_stats:get_top_msgq(20),
  {jiffy:encode(Stats), Req, State};

%% Graph node processes by reductions STATS
stats_json(Req, State=#state{mode = nodes}) ->
  Stats = process_stats:get_top_nodes(20),
  {jiffy:encode(Stats), Req, State}.
