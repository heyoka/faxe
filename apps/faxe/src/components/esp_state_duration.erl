%% Date: 15.07.2019 - 09:55
%% Ⓒ 2019 heyoka
%% @doc
%% Computes the duration of a given state. The state is defined via a lambda expression.
%% Timestamps for the duration are taken from the incoming data-point.
%%
%% For each consecutive point for which the lambda expression evaluates as true,
%% the state duration will be incremented by the duration between points.
%% When a point evaluates as false, the state duration is reset.
%%
%% The state duration will be added as an additional field to each point.
%% If the expression evaluates to false, the value will be -1.
%% When the lambda expression generates an error during evaluation, the point is discarded
%% and does not affect the state duration.
%%
-module(esp_state_duration).
-author("Alexander Minichmair").

%% API
-behavior(df_component).

-include("faxe.hrl").

-define(TOTAL_NAME, <<"_total">>).

%% API
-export([init/3, process/3, options/0]).

-record(state, {
   node_id,
   lambda,
   as,
   unit,
   last_state_timestamp = 0,
   emit_total = false,
   state_enter_time

}).

options() -> [
   {lambda, lambda},
   {as, binary, <<"state_duration">>},
   {unit, duration, <<"1s">>},
   {emit_total, is_set, false}
].

init(_NodeId, _Ins, #{lambda := Lambda, as := As, unit := Unit, emit_total := EmitTotal}) ->
   {ok, all, #state{lambda = Lambda, as = As, unit = Unit, emit_total = EmitTotal}}.

process(_In, #data_batch{points = _Points} = _Batch, _State = #state{lambda = _Lambda}) ->
   {error, not_implemented};
process(_Inport, #data_point{ts = Ts} = Point,
    State = #state{lambda = Lambda, last_state_timestamp = LastTs, as = As}) ->

   case process_point(Point, Lambda, LastTs) of
      {ok, Duration, NewLastTs} ->
         NewPoint = flowdata:set_field(Point, As, Duration),
         %% do we just go from state == true to false
         emit_total(LastTs, Duration, Ts, State),
         NewState =
         case Duration == 0 of
            true ->
               State#state{state_enter_time = Ts};
            false ->
               State
         end,
         {emit, NewPoint, NewState#state{last_state_timestamp = NewLastTs}};
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


emit_total(LastTs, Duration, Ts, State = #state{state_enter_time = StateEnterTs}) ->
   case LastTs > 0 andalso Duration == -1 of
      true ->
         maybe_build_total(Ts - StateEnterTs, State);
      false ->
         ok
   end.
%% @doc build a new data_point with the field {state.fieldname + "_total"} set to the total-duration
maybe_build_total(_D, #state{emit_total = false}) ->
   ok;
maybe_build_total(Duration, _State=#state{as = As}) ->
   P = #data_point{ts = faxe_time:now()},
   dataflow:emit(flowdata:set_field(P, <<"state_time_total">>, Duration)).

exec(Point, LFun) -> faxe_lambda:execute(Point, LFun).


