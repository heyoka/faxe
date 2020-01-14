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
-export([init/3, process/3, options/0]).

-record(state, {
   node_id,
   fields,
   field_states = []
}).

options() -> [{fields, binary_list}].

init(NodeId, _Ins, #{fields := Fields}) ->
   {ok, all, #state{node_id = NodeId, fields = Fields}}.

%%process(_In, #data_batch{points = Points} = Batch, State = #state{fields = Offset}) ->
%%%%   lager:info("~p Ts before: ~p",[?MODULE, [faxe_time:to_date(T0) || T0 <- flowdata:ts(Batch)]]),
%%%%   NewPoints = [execute(Point,Offset) || Point <- Points],
%%%%   NewBatch = flowdata:set_bounds(Batch#data_batch{points = NewPoints}),
%%%%   lager:info("~p Ts after: ~p",[?MODULE, [faxe_time:to_date(T1) || T1 <- flowdata:ts(NewBatch)]]),
%%%%   {emit, NewBatch, State}
%%ok
%%;
process(_Inport, P = #data_point{} = Point, State = #state{fields = FieldList, field_states = FS}) ->
   FieldVals = flowdata:fields(P, FieldList),
   Fields = lists:zip(FieldList, FieldVals),
   NewPoint = execute(Point, Fields, FieldList, FS),
%%   lager:notice("~p emitting: ~p when previous: ~p",[?MODULE, NewPoint, FS]),
   {emit, NewPoint, State#state{field_states = Fields}}.

%% @doc maps current datapoint fields to new values, depending on previous values and
%% fieldlist configuration
-spec execute(#data_point{}, list(binary()), list({binary(), number()}), list({binary(), number()}))
       -> #data_point{}.
execute(P =#data_point{}, FieldVals, FieldList, FieldStates) ->
   MapFn = fun({K, V}=Current) ->
      case {lists:member(K, FieldList), proplists:get_value(K, FieldStates)} of
         {_, undefined} -> Current;
         {false, _} -> Current;
         {true, PrevVal} -> {K, abs(V-PrevVal)}
      end
           end,
   Eval = lists:map(MapFn, FieldVals),
   flowdata:set_fields(P, Eval).


%%%%%%%%%%%%
-ifdef(TEST).
basic_test() ->
   P = #data_point{fields = #{<<"energy_used">> => 13.4563, <<"current_max">> => 3453.34534, <<"t1">> => 12}},
   LastValues = [{<<"current_max">>, 3753.34534}, {<<"t1">>, 12}],
   FieldList = [<<"current_max">>, <<"energy_used">>],
   FieldVals = flowdata:fields(P, FieldList),
   Fields = lists:zip(FieldList, FieldVals),
   ?assertEqual(#data_point{fields = #{<<"energy_used">> => 13.4563, <<"current_max">> => 300.0, <<"t1">> => 12}},
      execute(P, Fields, FieldList, LastValues)).

-endif.