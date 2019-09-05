%% Date: 06.06 - 19:04
%% Ⓒ 2019 heyoka
%% @doc
%% Filter points and batches with a lambda expression, which returns a boolean value;
%% for boolean value evaluation, see @see dfs_std_lib:bool/1
%%
-module(esp_where).
-author("Alexander Minichmair").

%% API
-behavior(df_component).

-include("faxe.hrl").
%% API
-export([init/3, process/3, options/0]).

-record(state, {
   node_id,
   lambda
}).

options() -> [{lambda, lambda}].

init(_NodeId, _Ins, #{lambda := Lambda} = O) ->
   lager:notice("Opts for ~p :: ~p", [?MODULE, O]),
   {ok, all, #state{lambda = Lambda}}.

process(_In, #data_batch{points = Points} = Batch, State = #state{lambda = Lambda}) ->
   case lists:filter(fun(Point) -> exec(Point, Lambda) end, Points) of
      [] -> {ok, State};
      NewList -> {emit, Batch#data_batch{points = NewList}, State}
   end;
process(_Inport, #data_point{} = Point, State = #state{lambda = Lambda}) ->
   case exec(Point, Lambda) of
      true -> {emit, Point, State};
      false -> {ok, State}
   end.

exec(Point, LFun) ->
   dfs_std_lib:bool(
      faxe_lambda:execute(Point, LFun)
   ).



