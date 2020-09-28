%% Date: 15.07.2019 - 09:55
%% Ⓒ 2019 heyoka
%% @doc
%% Computes the duration of a given state for all fields in a given object.
%% The state is defined via a lambda expression pattern.
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
-module(esp_state_change_bulk).
-author("Alexander Minichmair").

%% API
-behavior(df_component).

-include("faxe.hrl").

-define(TOTAL_NAME, <<"_total">>).
-define(PLACE_HOLDER, <<"$field">>).

%% API
-export([init/3, process/3, options/0, check_options/0, build_fun/2]).

-record(state, {
   node_id,
   lambda_pattern,
   state_lambdas = [],
   emit_entered,
   emit_left,
   entered_as,
   left_as,
   entered_keep = [],
   left_keep = [],
   unit,
   state_changes = [],
   prefix,
   field
}).

options() -> [
   {lambda_pattern, string},
   {field, string},
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

init(_NodeId, _Ins, #{lambda_pattern := Lambda_Pattern, enter_as := EnteredAs, leave_as := LeftAs, enter := EmitEntered,
   leave := EmitLeft, enter_keep := KeepFieldsEntered, leave_keep := KeepFieldsLeft, prefix := Prefix, field := Field}) ->

   {ok, all,
      #state{
         field = Field,
         lambda_pattern = Lambda_Pattern,
         state_lambdas = [],
         emit_entered = EmitEntered,
         emit_left = EmitLeft,
         entered_as = EnteredAs,
         left_as = LeftAs,
         entered_keep = KeepFieldsEntered,
         left_keep = KeepFieldsLeft,
         prefix = Prefix
         }}.

process(_In, #data_batch{points = _Points} = _Batch, _State = #state{state_lambdas = _Lambda}) ->
   {error, not_implemented};
process(_Inport, #data_point{} = Point, State = #state{field = Field}) ->
   FieldMap = flowdata:field(Point, Field),
%%   lager:notice("FieldMap:~p",[FieldMap]),
   StateTrackers = get_states(FieldMap, State),
   F = fun({FieldName, StateChange}, UpdatedStateChanges) ->
      case state_change:process(StateChange, Point) of
         {ok, NewStateChange} ->
            handle(state_change:get_state(NewStateChange), NewStateChange, State),
            [{FieldName, NewStateChange} | UpdatedStateChanges];
         {error, Error} ->
            lager:error("Error evaluating lambda: ~p",[Error]),
            [{FieldName, StateChange} | UpdatedStateChanges]
      end
      end,
   NewStates = lists:foldl(F, [], StateTrackers),
   {ok, State#state{state_changes = NewStates}}.

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

emit_point_data(P, Keep, AddFieldNames, AddFieldVals, _State = #state{prefix = Prefix}) ->
   Fields = flowdata:fields(P, Keep),
   NewPoint = P#data_point{fields = #{}, tags = #{}},
   FieldNames = [<<Prefix/binary, F/binary>> || F <- AddFieldNames],
   dataflow:emit(flowdata:set_fields(NewPoint, FieldNames++Keep, AddFieldVals++Fields)).


get_states(FieldMap, #state{lambda_pattern = Pattern, state_changes = States}) ->
   Fields = maps:keys(FieldMap),
   lager:notice("fields: ~p",[Fields]),
   lists:map(
      fun
         (FieldName) ->
         case proplists:get_value(FieldName, States) of
            undefined ->
               Fun = build_fun(Pattern, FieldName),
               {FieldName, state_change:new(Fun)};
            Fun ->
               {FieldName, Fun}
         end
      end,
      Fields).

build_fun(PatternString, FieldName ) when is_binary(PatternString) ->
   FunString = binary:replace(PatternString, ?PLACE_HOLDER, string:titlecase(FieldName)),
   lager:notice("Fun: ~p",[FunString]),
   VarBindings = [{string:titlecase(Var), list_to_binary(string:lowercase(Var))} || Var <- [binary_to_list(FieldName)]],
   {Vars, Bindings} = lists:unzip(VarBindings),
   faxe_dfs:make_lambda_fun(binary_to_list(FunString), Vars, Bindings).


