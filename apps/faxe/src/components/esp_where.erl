%% Date: 06.06 - 19:04
%% Ⓒ 2019 heyoka
%% @doc
%% Filter points and batches with a lambda expression, which returns a boolean value;
%% Data-items for which the lambda expression evaluates as false, will be discarded.
%% For boolean value evaluation, @see dfs_std_lib:bool/1
%%
-module(esp_where).
-author("Alexander Minichmair").
%% API
-behavior(df_component).
-include("faxe.hrl").

-export([init/3, process/3, options/0]).

-record(state, {
   node_id              :: binary(),
   lambda               :: undefined|#faxe_lambda{},
   emit_empty = false   :: true|false
}).

options() -> [
   {lambda, lambda},
   {emit_empty, boolean, false}
].

init(_NodeId, _Ins, #{lambda := Lambda, emit_empty := Empty}) ->
   {ok, all, #state{lambda = Lambda, emit_empty = Empty}}.

process(_In, #data_batch{points = Points} = Batch, State = #state{lambda = Lambda}) ->
   Res = lists:filter(fun(Point) -> exec(Point, Lambda) end, Points),
   maybe_emit(Batch, Res, State);
process(_Inport, #data_point{} = Point, State = #state{lambda = Lambda}) ->
   case exec(Point, Lambda) of
      true -> {emit, Point, State};
      false -> {ok, State}
   end.

exec(Point, LFun) ->
   dfs_std_lib:bool( faxe_lambda:execute(Point, LFun) ).

maybe_emit(_B, [], State = #state{emit_empty = false}) ->
   {ok, State};
maybe_emit(Batch, NewList, State = #state{}) ->
   {emit, Batch#data_batch{points = NewList}, State}.




