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
-module(esp_state_change).
-author("Alexander Minichmair").

%% API
-behavior(df_component).

-include("faxe.hrl").

-define(TOTAL_NAME, <<"_total">>).

%% API
-export([init/3, process/3, options/0, check_options/0]).

-record(state, {
   node_id,
   state_lambda,
   emit_entered,
   emit_left,
   entered_as,
   left_as,
   entered_keep = [],
   left_keep = [],
   unit,
   state_change,
   prefix
}).

options() -> [
   {lambda, lambda},
   {enter_as, binary, <<"state_entered">>},
   {leave_as, binary, <<"state_left">>},
   {enter, is_set, undefined},
   {leave, is_set, undefined},
   {enter_keep, string_list, []},
   {leave_keep, string_list, []},
   {prefix, string, <<"">>},
   {unit, duration, <<"1s">>}
].

check_options() ->
   [
      {oneplus_of_params, [enter, leave]}
   ].

init(_NodeId, _Ins, #{lambda := Lambda, enter_as := EnteredAs, leave_as := LeftAs, enter := EmitEntered,
   leave := EmitLeft, enter_keep := KeepFieldsEntered, leave_keep := KeepFieldsLeft, prefix := Prefix}) ->
   StateTracker = state_change:new(Lambda),
   {ok, all,
      #state{
         state_lambda = Lambda,
         emit_entered = EmitEntered,
         emit_left = EmitLeft,
         entered_as = EnteredAs,
         left_as = LeftAs,
         entered_keep = KeepFieldsEntered,
         left_keep = KeepFieldsLeft,
         state_change = StateTracker,
         prefix = Prefix
         }}.

process(_In, #data_batch{points = _Points} = _Batch, _State = #state{state_lambda = _Lambda}) ->
   {error, not_implemented};
process(_Inport, #data_point{} = Point, State = #state{state_change = StateChange}) ->

   case state_change:process(StateChange, Point) of
      {ok, NewStateChange} ->
         handle(state_change:get_state(NewStateChange), NewStateChange, State#state{state_change = NewStateChange});
      {error, Error} ->
         lager:error("Error evaluating lambda: ~p",[Error]),
         {ok, State}
   end.

handle(entered, StateState, State=#state{emit_entered = true, entered_as = As, entered_keep = Keep}) ->
   P = state_change:get_last_point(StateState),
   emit_point_data(P, Keep, [As], [1], State);
handle(left, StateState, State=#state{emit_left = true, left_as = As, left_keep = Keep}) ->
   P = state_change:get_last_point(StateState),
   AddFNames = [
      As,
      <<"state_start_ts">>,
      <<"state_end_ts">>,
      <<"state_duration">>,
      <<"state_count">>],
   AddFields = [
      1,
      state_change:get_last_enter_time(StateState),
      P#data_point.ts,
      state_change:get_last_duration(StateState),
      state_change:get_last_count(StateState)],

   emit_point_data(P, Keep, AddFNames, AddFields, State);
handle(_, _StateState, State=#state{}) ->
   {ok, State}.

emit_point_data(P, Keep, AddFieldNames, AddFieldVals, State = #state{prefix = Prefix}) ->
   Fields = flowdata:fields(P, Keep),
   NewPoint = P#data_point{fields = #{}, tags = #{}},
   FieldNames = [<<Prefix/binary, F/binary>> || F <- AddFieldNames],
   {emit, flowdata:set_fields(NewPoint, FieldNames++Keep, AddFieldVals++Fields), State}.