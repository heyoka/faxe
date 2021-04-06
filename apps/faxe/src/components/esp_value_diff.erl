%% Date: 05.01.17 - 14:11
%% Ⓒ 2017 heyoka
%%
%% @doc
%% Instead of the current value outputs the difference to a previous value for multiple fields
%% if no previous value is found for a specific field, the current field-value is used
%% @end
-module(esp_value_diff).
-author("Alexander Minichmair").

%% API
-behavior(df_component).

-include("faxe.hrl").
%% API
-export([init/3, process/3, options/0, check_options/0]).

-define(MODE_ABS, <<"abs">>).
-define(MODE_CP, <<"c-p">>).
-define(MODE_PC, <<"p-c">>).

-record(state, {
   node_id,
   fields,
   as,
   default,
   field_states = [],
   diff_fun
}).

options() -> [
   {fields, string_list},
   {as, string_list, undefined},
   {default, number, undefined},
   {mode, string, ?MODE_ABS}
].

check_options() ->
   [{one_of, mode, [?MODE_ABS, ?MODE_CP, ?MODE_PC]}].

init(NodeId, _Ins, #{fields := Fields, as := As0, default := Default, mode := Mode}) ->
   As1 = case As0 of undefined -> Fields; _ -> As0 end,
   As = lists:zip(Fields, As1),
   DiffFun = diff_fun(Mode),
   {ok, all, #state{node_id = NodeId, fields = Fields, as = As, default = Default, diff_fun = DiffFun}}.

process(_Inport, P = #data_point{} = Point, State = #state{fields = FieldList}) ->
   FieldVals = flowdata:fields(P, FieldList),
   Fields = lists:zip(FieldList, FieldVals),
   NewPoint = execute(Point, Fields, State),
   {emit, NewPoint, State#state{field_states = Fields}}.

%% @doc maps current datapoint fields to new values, depending on previous values and
%% fieldlist configuration
-spec execute(#data_point{}, list(binary()), #state{})
       -> #data_point{}.
execute(P =#data_point{}, FieldVals,
    #state{fields = FieldList, field_states = FieldStates, default = Def, as = As, diff_fun = Fun}) ->
   FoldFn =
   fun
      ({_K, undefined}, NewFields) ->
         %% this means there is no such field in the data-point, ignore it
         NewFields;
      ({K, V}, NewFields) ->
         case lists:member(K, FieldList) of
            true ->
               NewValue =
                  case proplists:get_value(K, FieldStates) of
                     %% we do not have any previous value yet, use default value
                     undefined -> default(V, Def);
                     PrevVal when is_number(V), is_number(PrevVal) -> Fun(V, PrevVal);
                     Other -> erlang:error({cannot_diff_values, V, Other})
                  end,
               [{proplists:get_value(K, As), NewValue}|NewFields];
            false -> NewFields
         end
   end,
   Eval = lists:foldl(FoldFn, [], FieldVals),
   flowdata:set_fields(P, Eval).


default(Val, undefined) -> Val;
default(_Val, Default) -> Default.

diff_fun(?MODE_ABS)  -> fun(Curr, Prev) -> abs(Curr-Prev) end;
diff_fun(?MODE_CP)   -> fun(Curr, Prev) -> Curr-Prev end;
diff_fun(?MODE_PC)   -> fun(Curr, Prev) -> Prev-Curr end.

%%%%%%%%%%%%
-ifdef(TEST).
basic_test() ->
   P = test_point(),
   LastValues = [{<<"current_max">>, 3753.34534}, {<<"t1">>, 12}],
   FieldList = [<<"current_max">>, <<"energy_used">>],
   FieldVals = flowdata:fields(P, FieldList),
   Fields = lists:zip(FieldList, FieldVals),
   ?assertEqual(#data_point{fields = #{<<"energy_used">> => 13.4563, <<"current_max">> => 300.0, <<"t1">> => 12}},
      execute(P, Fields,
         #state{fields = FieldList, field_states = LastValues,
            as = lists:zip(FieldList, FieldList), default = undefined, diff_fun = diff()})).

default_test() ->
   P = test_point(),
   LastValues = [{<<"current_max">>, 3753.34534}, {<<"t1">>, 12}],
   FieldList = [<<"current_max">>, <<"energy_used">>],
   FieldVals = flowdata:fields(P, FieldList),
   Fields = lists:zip(FieldList, FieldVals),
   ?assertEqual(#data_point{fields = #{<<"energy_used">> => 0, <<"current_max">> => 300.0, <<"t1">> => 12}},
      execute(P, Fields,
         #state{fields = FieldList, field_states = LastValues,
            as = lists:zip(FieldList, FieldList), default = 0, diff_fun = diff()})).

fields_not_present_test() ->
   P = test_point(),
   LastValues = [{<<"current_max">>, 3753.34534}, {<<"t1">>, 12}],
   FieldList = [<<"current_max">>, <<"energy_used">>, <<"somefieldname">>],
   FieldVals = flowdata:fields(P, FieldList),
   Fields = lists:zip(FieldList, FieldVals),
   ?assertEqual(#data_point{fields = #{<<"energy_used">> => 0, <<"current_max">> => 300.0, <<"t1">> => 12}},
      execute(P, Fields,
         #state{fields = FieldList, field_states = LastValues,
            as = lists:zip(FieldList, FieldList), default = 0, diff_fun = diff()})).

as_test() ->
   P = test_point(),
   LastValues = [{<<"current_max">>, 3753.34534}, {<<"t1">>, 12}],
   FieldList = [<<"current_max">>, <<"energy_used">>],
   AsList = [<<"current_max_diff">>, <<"energy_used_diff">>],
   FieldVals = flowdata:fields(P, FieldList),
   Fields = lists:zip(FieldList, FieldVals),
   ?assertEqual(#data_point{fields = #{<<"current_max">> => 3453.34534,
      <<"current_max_diff">> => 300.0,
      <<"energy_used">> => 13.4563,
      <<"energy_used_diff">> => 99,<<"t1">> => 12}},
      execute(P, Fields,
         #state{fields = FieldList, field_states = LastValues,
            as = lists:zip(FieldList, AsList), default = 99, diff_fun = diff()})).

test_point() ->
   #data_point{fields = #{<<"energy_used">> => 13.4563, <<"current_max">> => 3453.34534, <<"t1">> => 12}}.
diff() -> diff_fun(?MODE_ABS).

-endif.