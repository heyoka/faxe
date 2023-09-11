%% Date: 15.07.2019 - 09:55
%% Ⓒ 2019 heyoka
%% @doc
%% Computes the duration of a given state for all fields in a given object.
%% It is assumed that 'field' is an object containing only plain fields (no other objects or lists).
%% The state is defined via a lambda expression pattern (example: '$field =< 2 orelse $field == 222').
%% Note that the lambda pattern must be valid erlang code and only all $field placeholders will be replace by the current fieldname.
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
-export([init/3, process/3, options/0, check_options/0, build_fun/2, wants/0, emits/0]).

-record(state, {
   node_id,
   lambda_pattern,
   state_value,
   state_lambdas = [],
   emit_entered,
   emit_left,
   entered_as,
   left_as,
   state_id_as,
   entered_keep = [],
   left_keep = [],
   unit,
   state_changes = [],
   prefix,
   field,
   excluded = [],
   last_data
}).

options() -> [
   {lambda_pattern, string, undefined},
   {state_value, any, undefined},
   {field, string},
   {exclude_fields, string_list, []},
   {enter_as, binary, <<"state_entered">>},
   {leave_as, binary, <<"state_left">>},
   {state_id_as, binary, <<"state_id">>},
   {enter, is_set, undefined},
   {leave, is_set, undefined},
   {enter_keep, string_list, []},
   {leave_keep, string_list, []},
   {prefix, string, <<"">>},
   {unit, duration, <<"1s">>}
].

check_options() ->
   [
      {oneplus_of_params, [enter, leave]},
      {one_of_params, [lambda_pattern, state_value]}
   ].

wants() -> point.
emits() -> point.

init(_NodeId, _Ins, #{lambda_pattern := Lambda_Pattern, enter_as := EnteredAs, leave_as := LeftAs, enter := EmitEntered,
   leave := EmitLeft, enter_keep := KeepFieldsEntered, leave_keep := KeepFieldsLeft, prefix := Prefix, field := Field,
   exclude_fields := Excluded, state_value := StateValue, state_id_as := SIdAs}) ->

   {ok, all,
      #state{
         field = Field,
         excluded = Excluded,
         lambda_pattern = Lambda_Pattern,
         state_value = StateValue,
         state_lambdas = [],
         emit_entered = EmitEntered,
         emit_left = EmitLeft,
         entered_as = EnteredAs,
         left_as = LeftAs,
         state_id_as = SIdAs,
         entered_keep = KeepFieldsEntered,
         left_keep = KeepFieldsLeft,
         prefix = Prefix
         }}.

process(_In, #data_batch{points = _Points} = _Batch, _State = #state{state_lambdas = _Lambda}) ->
   {error, not_implemented};
process(_Inport, #data_point{} = Point, State = #state{field = Field}) ->
   FieldMap = flowdata:field(Point, Field),
   maybe_process(FieldMap, Point, State).

maybe_process(FieldMap, _Point, State=#state{last_data = FieldMap}) ->
   {ok, State};
maybe_process(FieldMap, Point, State = #state{}) ->
   StateTrackers = get_states(FieldMap, State),
   F = fun({FieldName, StateChange}, UpdatedStateChanges) ->
      case state_change:process(StateChange, Point) of
         {ok, NewStateChange} ->
            Handeled = handle(state_change:get_state(NewStateChange), FieldName, NewStateChange, State),
            [{{FieldName, NewStateChange}, Handeled} | UpdatedStateChanges];
         {error, Error} ->
            lager:error("Error evaluating lambda: ~p",[Error]),
            [{FieldName, StateChange, undefined} | UpdatedStateChanges]
      end
       end,
   Results = plists:fold(F, [], StateTrackers, {processes, schedulers}),
   {NewStates, Emits} = lists:unzip(Results),
   lists:foreach(fun
                    (P=#data_point{}) -> dataflow:emit(P);
                    (undefined) -> ok
                 end, Emits),
   {ok, State#state{state_changes = NewStates, last_data = FieldMap}}.

handle(entered, FieldName, StateState,
    State=#state{emit_entered = true, entered_as = As, entered_keep = Keep, state_id_as = SIdAs}) ->
   P = state_change:get_last_point(StateState),
   PFields = [As, <<"field">>, SIdAs, <<"state_start_ts">>],
   PVals = [1, FieldName, state_change:get_state_id(StateState), P#data_point.ts],
   prepare_point_data(P, Keep, PFields, PVals, State);
handle(left, FieldName, StateState,
    State=#state{emit_left = true, left_as = As, left_keep = Keep, state_id_as = SIdAs}) ->
   P = state_change:get_last_point(StateState),
   AddFNames = [
      As,
      SIdAs,
      <<"state_start_ts">>,
      <<"state_end_ts">>,
      <<"state_duration">>,
      <<"state_count">>,
      <<"field">>],
   AddFields = [
      1,
      state_change:get_state_id(StateState),
      state_change:get_last_enter_time(StateState),
      state_change:get_end(StateState),
      state_change:get_last_duration(StateState),
      state_change:get_last_count(StateState),
      FieldName],

%%   lager:notice("state_left ~nlast ~p~n end ~p",[P#data_point.ts, state_change:get_end(StateState)]),

   prepare_point_data(P#data_point{ts = state_change:get_end(StateState)}, Keep, AddFNames, AddFields, State);
handle(_, _F, _StateState, #state{}) ->
   undefined.

prepare_point_data(P, Keep, AddFieldNames, AddFieldVals, _State = #state{prefix = Prefix}) ->
   Fields = flowdata:fields(P, Keep),
   NewPoint = P#data_point{fields = #{}, tags = #{}},
   FieldNames = [<<Prefix/binary, F/binary>> || F <- AddFieldNames],
   flowdata:set_fields(NewPoint, FieldNames++Keep, AddFieldVals++Fields).


get_states(FieldMap, S=#state{state_changes = States, field = ParentField, excluded = Ex}) ->
   Fields = maps:keys(FieldMap),
   plists:fold(
      fun
         (FieldName0, CurrentStates) ->
            FieldName = <<ParentField/binary, ".", FieldName0/binary>>,
            case lists:member(FieldName, Ex) of
               true ->
                  CurrentStates;
               false ->
                  case proplists:get_value(FieldName, States) of
                     undefined ->
                        Fun = build_fun(S, FieldName),
                        [{FieldName, state_change:new(Fun, FieldName)}|CurrentStates];
                     _Fun ->
                        CurrentStates
                  end
            end
      end,
      States,
      Fields,
      {processes, schedulers}).

build_fun(#state{lambda_pattern = undefined, state_value = Val}, _FieldName) ->
   Val;
build_fun(#state{lambda_pattern = PatternString}, FieldName) when is_binary(PatternString) ->
   Name = clean_param_name(FieldName),
   FunString = binary:replace(PatternString, ?PLACE_HOLDER, string:titlecase(Name), [global]),
   Vars = [string:titlecase(binary_to_list(Name))],
   Bindings = [FieldName],
   L = faxe_dfs:make_lambda_fun(binary_to_list(FunString), Vars, Bindings),
   faxe_lambda:ensure(L).

clean_param_name(Name) when is_binary(Name) ->
   S0 = re:replace(Name, "[^a-zA-Z0-9_.]", <<"_">>, [{return, binary}]),
   binary:replace(S0, [<<".">>,<<"[">>,<<"]">>], <<"_">>, [global]).


