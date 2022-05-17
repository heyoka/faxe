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
   entered_keep = [],
   left_keep = [],
   entered_keep_as = [],
   left_keep_as = [],
   keep = [],
   keep_as = [],
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
   {enter_keep_as, string_list, []},
   {leave_keep_as, string_list, []},
   {keep, string_list, []},
   {keep_as, string_list, []},
   {prefix, string, <<"">>},
   {unit, duration, <<"1s">>}
].

check_options() ->
   [
      {oneplus_of_params, [enter, leave]}
   ].

wants() -> point.
emits() -> point.

init(_NodeId, _Ins, #{lambda := Lambda, enter_as := EnteredAs, leave_as := LeftAs,
   enter := EmitEntered,
   leave := EmitLeft,
   enter_keep := KeepFieldsEntered, enter_keep_as := KeepFieldsEnteredAliases,
   leave_keep := KeepFieldsLeft, leave_keep_as := KeepFieldsLeftAliases,
   prefix := Prefix,
   keep := Keep, keep_as := KeepAliases}) ->

   StateTracker = state_change:new(Lambda),

   EnteredKeepAs = aliased(KeepFieldsEntered, KeepFieldsEnteredAliases),
   LeftKeepAs = aliased(KeepFieldsLeft, KeepFieldsLeftAliases),
   KeepAs = aliased(Keep, KeepAliases),

   State = #state{
      state_lambda = Lambda,
      emit_entered = EmitEntered,
      emit_left = EmitLeft,
      entered_as = EnteredAs,
      left_as = LeftAs,
      entered_keep = KeepFieldsEntered,
      entered_keep_as = EnteredKeepAs,
      left_keep = KeepFieldsLeft,
      left_keep_as = LeftKeepAs,
      keep = Keep,
      keep_as = KeepAs,
      state_change = StateTracker,
      prefix = Prefix
   },
   NewState = eval_keep(State),
   lager:notice("State: ~p",[lager:pr(NewState, ?MODULE)]),
   {ok, all, NewState}.


aliased(FList, []) ->
   FList;
aliased(_FList, Aliases) ->
   Aliases.


eval_keep(State = #state{
      entered_keep = EKeep, left_keep = LKeep,
      entered_keep_as = EnteredAs, left_keep_as = LeftAs,
      keep = KeepAll, keep_as = KeepAllAs}) ->

   EnteredKeepFields = lists:zip(EKeep, EnteredAs),
   LeftKeepFields = lists:zip(LKeep, LeftAs),
   AllKeepField = lists:zip(KeepAll, KeepAllAs),

   EnteredKeep = sets:to_list(sets:from_list(EnteredKeepFields++AllKeepField)),
   LeftKeep = sets:to_list(sets:from_list(LeftKeepFields++AllKeepField)),
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

handle(entered, StateState, State=#state{emit_entered = true, entered_as = As, entered_keep = Keep}) ->
   P = state_change:get_last_point(StateState),
   emit_point_data(P, Keep, [{As, 1}], State);
handle(left, StateState, State=#state{emit_left = true, left_as = As, left_keep = Keep}) ->
   P = state_change:get_last_point(StateState),
   AddFields = [
      {As, 1},
      {<<"state_start_ts">>, state_change:get_last_enter_time(StateState)},
      {<<"state_end_ts">>, P#data_point.ts},
      {<<"state_duration">>, state_change:get_last_duration(StateState)},
      {<<"state_count">>, state_change:get_last_count(StateState)}
   ],
   emit_point_data(P, Keep, AddFields, State);
handle(_, _StateState, State=#state{}) ->
   {ok, State}.

emit_point_data(P, Keep, AddFields, State = #state{prefix = Prefix}) ->
   {KeepFieldNames, KeepFieldAliases} = lists:unzip(Keep),
   FieldValues = flowdata:fields(P, KeepFieldNames),
   PrefixedFieldNames = [<<Prefix/binary, F/binary>> || F <- KeepFieldAliases],
   Fields = lists:zip(PrefixedFieldNames, FieldValues),
   NewPoint = P#data_point{fields = #{}, tags = #{}},
   {emit, flowdata:set_fields(NewPoint, Fields++AddFields), State}.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% TESTS %%%%%%%%%%%%%%%%%%%%%%%%%%
-ifdef(TEST).

eval_keep_1_test() ->
   EKeep = [],
   LKeep0 = [<<"data.err.SysNo">>],
   Keep0 = [<<"data.err.Mod">>],
   Keep = lists:zip(Keep0, Keep0),
   LKeep = lists:zip(LKeep0, LKeep0),
   S = #state{keep = Keep0, entered_keep = EKeep, left_keep = LKeep0},
   Ex = S#state{entered_keep = Keep, left_keep = LKeep++Keep},
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