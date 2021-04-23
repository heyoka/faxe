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
-export([init/3, process/3, options/0, wants/0, emits/0]).

-record(state, {
   node_id,
   lambda,
   as,
   unit,
   emit_total = false,
   state_change

}).

options() -> [
   {lambda, lambda},
   {as, binary, <<"state_duration">>},
   {unit, duration, <<"1s">>},
   {emit_total, is_set, false}
].

wants() -> point.
emits() -> point.

init(_NodeId, _Ins, #{lambda := Lambda, as := As, unit := Unit, emit_total := EmitTotal}) ->
   StateTrack = state_change:new(Lambda),
   {ok, all, #state{lambda = Lambda, as = As, unit = Unit, emit_total = EmitTotal, state_change = StateTrack}}.

process(_In, #data_batch{points = _Points} = _Batch, _State = #state{lambda = _Lambda}) ->
   {error, not_implemented};
process(_Inport, #data_point{} = Point, State = #state{ as = As, state_change = StateTrack}) ->

   case state_change:process(StateTrack, Point) of
      {ok, NewStateTrack} ->
         Duration = state_change:get_duration(NewStateTrack),
         NewPoint = flowdata:set_field(Point, As, Duration),
         {emit, NewPoint, State#state{state_change = NewStateTrack}};
      {error, Error} ->
         lager:error("Error evaluating lambda: ~p",[Error]),
         {ok, State}
   end.

