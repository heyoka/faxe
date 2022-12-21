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
-export([init/3, process/3, options/0, check_options/0, wants/0, emits/0]).

-record(state, {
   node_id,
   state_lambda,
   emit_entered,
   emit_left,
   entered_as,
   left_as,
   state_id_as,
   entered_keep = [],
   left_keep = [],
   keep = [],
   unit,
   state_change,
   prefix
}).

options() -> [
   {lambda, lambda},
   {enter_as, string, <<"state_entered">>},
   {leave_as, string, <<"state_left">>},
   {state_id_as, string, <<"state_id">>},
   {enter, is_set, undefined},
   {leave, is_set, undefined},
   {enter_keep, string_list, []},
   {leave_keep, string_list, []},
   {keep, string_list, []},
   {prefix, string, <<"">>},
   {unit, duration, <<"1s">>}
].

check_options() ->
   [
      {oneplus_of_params, [enter, leave]}
   ].

wants() -> point.
emits() -> point.

init(_NodeId, _Ins, #{lambda := Lambda, enter_as := EnteredAs, leave_as := LeftAs, enter := EmitEntered,
   state_id_as := SIdAs, leave := EmitLeft, enter_keep := KeepFieldsEntered, leave_keep := KeepFieldsLeft,
   prefix := Prefix, keep := Keep}) ->
   StateTracker = state_change:new(Lambda),
   State = #state{
      state_lambda = Lambda,
      emit_entered = EmitEntered,
      emit_left = EmitLeft,
      entered_as = EnteredAs,
      left_as = LeftAs,
      state_id_as = SIdAs,
      entered_keep = KeepFieldsEntered,
      left_keep = KeepFieldsLeft,
      keep = Keep,
      state_change = StateTracker,
      prefix = Prefix
   },
   NewState = eval_keep(State),
   {ok, all, NewState}.


eval_keep(State = #state{entered_keep = EKeep, left_keep = LKeep, keep = KeepAll}) ->
   EnteredKeep = sets:to_list(sets:from_list(EKeep++KeepAll)),
   LeftKeep = sets:to_list(sets:from_list(LKeep++KeepAll)),
   State#state{
      entered_keep = EnteredKeep,
      left_keep = LeftKeep
   }.

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

handle(entered, StateState,
    State=#state{emit_entered = true, entered_as = As, entered_keep = Keep, state_id_as = SIdAs}) ->
   P = state_change:get_last_point(StateState),
   emit_point_data(P, Keep, [As, SIdAs], [1, state_change:get_state_id(StateState)], State);
handle(left, StateState, State=#state{emit_left = true, left_as = As, left_keep = Keep, state_id_as = SIdAs}) ->
   P = state_change:get_last_point(StateState),
   AddFNames = [
      As,
      SIdAs,
      <<"state_start_ts">>,
      <<"state_end_ts">>,
      <<"state_duration">>,
      <<"state_count">>],
   AddFields = [
      1,
      state_change:get_state_id(StateState),
      state_change:get_last_enter_time(StateState),
      state_change:get_end(StateState),
      state_change:get_last_duration(StateState),
      state_change:get_last_count(StateState)],

   emit_point_data(P#data_point{ts = state_change:get_end(StateState)}, Keep, AddFNames, AddFields, State);
handle(_, _StateState, State=#state{}) ->
   {ok, State}.

emit_point_data(P, Keep, AddFieldNames, AddFieldVals, State = #state{prefix = Prefix}) ->
   Fields = flowdata:fields(P, Keep),
   NewPoint = P#data_point{fields = #{}, tags = #{}},
   FieldNames = [<<Prefix/binary, F/binary>> || F <- AddFieldNames],
   {emit, flowdata:set_fields(NewPoint, FieldNames++Keep, AddFieldVals++Fields), State}.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% TESTS %%%%%%%%%%%%%%%%%%%%%%%%%%
-ifdef(TEST).

eval_keep_1_test() ->
   EKeep = [],
   LKeep = [<<"data.err.SysNo">>],
   Keep = [<<"data.err.Mod">>],
   S = #state{keep = Keep, entered_keep = EKeep, left_keep = LKeep},
   Ex = S#state{entered_keep = Keep, left_keep = [<<"data.err.SysNo">>,<<"data.err.Mod">>]},
   ?assertEqual(Ex, eval_keep(S)).

eval_keep_2_test() ->
   EKeep = [<<"data.err.SysNo">>],
   LKeep = [<<"data.err.SysNo">>],
   Keep = [],
   S = #state{keep = Keep, entered_keep = EKeep, left_keep = LKeep},
   Ex = S#state{entered_keep = [<<"data.err.SysNo">>], left_keep = [<<"data.err.SysNo">>]},
   ?assertEqual(Ex, eval_keep(S)).

eval_keep_3_test() ->
   EKeep = [<<"data.err.SysNo">>],
   LKeep = [<<"data.err.SysNo">>, <<"data.err.ErrCode">>],
   Keep = [<<"data.err.Mod">>],
   S = #state{keep = Keep, entered_keep = EKeep, left_keep = LKeep},
   Ex = S#state{
      entered_keep = [<<"data.err.SysNo">>, <<"data.err.Mod">>],
      left_keep = [<<"data.err.ErrCode">>, <<"data.err.SysNo">>, <<"data.err.Mod">>]},
   ?assertEqual(Ex, eval_keep(S)).

eval_keep_4_test() ->
   EKeep = [],
   LKeep = [],
   Keep = [<<"data.err.SysNo">>, <<"data.err.ErrCode">>, <<"data.err.Mod">>],
   ExAll = [<<"data.err.ErrCode">>, <<"data.err.SysNo">>, <<"data.err.Mod">>],
   S = #state{keep = Keep, entered_keep = EKeep, left_keep = LKeep},
   Ex = S#state{entered_keep = ExAll, left_keep = ExAll},
   ?assertEqual(Ex, eval_keep(S)).

eval_keep_unique_test() ->
   EKeep = [<<"data.err.SysNo">>],
   LKeep = [<<"data.err.Mod">>],
   Keep = [<<"data.err.SysNo">>, <<"data.err.ErrCode">>, <<"data.err.Mod">>],
   ExAll = [<<"data.err.ErrCode">>, <<"data.err.SysNo">>, <<"data.err.Mod">>],
   S = #state{keep = Keep, entered_keep = EKeep, left_keep = LKeep},
   Ex = S#state{entered_keep = ExAll, left_keep = ExAll},
   ?assertEqual(Ex, eval_keep(S)).

-endif.