%% Date: 15.07.2019 - 09:55
%% Ⓒ 2019 heyoka
%% @doc
%% Computes the duration of a given state. The state is defined via a lambda expression.
%% For each consecutive point for which the expression evaluates as true,
%% the state duration will be incremented by the duration between points.
%% When a point evaluates as false, the state duration is reset.
%%
%% The state duration will be added as an additional field to each point.
%% If the expression evaluates to false, the value will be -1.
%% If the lambda expression generates an error during evaluation, the point is discarded
%% and does not affect the state duration.
%%
-module(esp_state_duration).
-author("Alexander Minichmair").

%% API
-behavior(df_component).

-include("faxe.hrl").
%% API
-export([init/3, process/3, options/0]).

-record(state, {
   node_id,
   lambda,
   as,
   unit,
   last_state_timestamp = 0

}).

options() -> [{lambda, lambda}, {as, binary, <<"state_duration">>}, {unit, duration, <<"1s">>}].

init(_NodeId, _Ins, #{lambda := Lambda, as := As, unit := Unit}) ->
   {ok, all, #state{lambda = Lambda, as = As, unit = Unit}}.

process(_In, #data_batch{points = _Points} = _Batch, _State = #state{lambda = _Lambda}) ->
   {error, not_implemented};
process(_Inport, #data_point{} = Point,
    State = #state{lambda = Lambda, last_state_timestamp = LastTs, as = As}) ->
   case process_point(Point, Lambda, LastTs) of
      {ok, Duration, NewLastTs} ->
         NewPoint = flowdata:set_field(Point, As, Duration),
         {emit, NewPoint, State#state{last_state_timestamp = NewLastTs}};
      {error, Error} ->
         lager:error("Error evaluating lambda: ~p",[Error]),
         {ok, State}
   end.


-spec process_point(#data_point{}, function(), non_neg_integer()) ->
   {Duration :: integer(), LastTs :: non_neg_integer()}.
process_point(Point=#data_point{ts = Ts}, LFun, LastTs) ->
   case (catch exec(Point, LFun)) of
      true when LastTs > 0 -> {ok, Ts - LastTs, LastTs};
      true -> {ok, 0, Ts};
      false -> {ok, -1, 0};
      Error -> {error, Error}
   end.

exec(Point, LFun) -> faxe_lambda:execute(Point, LFun).


