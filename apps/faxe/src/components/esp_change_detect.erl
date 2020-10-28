%% Date: 06.06 - 19:04
%% Ⓒ 2019 heyoka
%% @doc emits new point-values only if different from the previous point
%%
%% * multiple fields can be monitored by this node, if no field is specified,
%% the whole datapoint will be used for comparison
%%
%% if nothing is given for 'fields', the complete data-item is compared to the last one
%%
%% * if reset_timeout is given, all previous values are reset, if there are no points
%% coming in for this amount of time
%%
%% resetting a value means the next time a new value comes in, there is no last value to compare to, thus
%% the node will always emit after a reset_timeout
%%
%% * if timeout is given, it specifies an interval where previous values are reset, regardless of incoming data
%%
%%
%% * for value comparison erlang's strict equals (=:=) is used, so 1.0 is not equal to 1
%%
-module(esp_change_detect).
-author("Alexander Minichmair").

%% API
-behavior(df_component).

-include("faxe.hrl").
%% API
-export([init/3, process/3, options/0, handle_info/2]).

-record(state, {
   node_id,
   fields,
   values = [],
   reset_timeout,
   timeout,
   reset_timer,
   timer
}).

options() -> [
   {fields, binary_list, undefined},
   {reset_timeout, duration, undefined},
   {timeout, duration, undefined}
].

init(_NodeId, _Ins, #{fields := FieldList, reset_timeout := ResetTimeout, timeout := TOut}) ->
   ResetTime = timer_interval(ResetTimeout),
   TimeOut = timer_interval(TOut),
   Timer = start_timeout(TimeOut),
   {ok, all, #state{fields = FieldList, reset_timeout = ResetTime, timeout = TimeOut, timer = Timer}}.

process(_In, #data_batch{points = Points} = Batch,
    State = #state{fields = FieldNames, values = Vals, reset_timer = TRef, reset_timeout = Time}) ->
   cancel_timer(TRef),
   {NewPoints, LastValues} = process_points(Points, [], Vals, FieldNames),
   NewState = State#state{values = LastValues, reset_timer = reset_timeout(Time)},
   case NewPoints of
      [] -> {ok, NewState};
      Es when is_list(Es) -> {emit, Batch#data_batch{points = NewPoints}}
   end;
process(_Inport, #data_point{} = Point,
    State = #state{fields = Fields, values = LastValues, reset_timer = TRef, reset_timeout = Time}) ->
   cancel_timer(TRef),
   {Filtered, NewValues} = process_point(Point, LastValues, Fields),
%%   lager:info("new last values: ~p" ,[NewValues]),
   NewState = State#state{values = NewValues, reset_timer = reset_timeout(Time)},
   case Filtered of
      Point -> {emit, Point, NewState};
      _ -> {ok, NewState}
   end
   .


handle_info(reset_timeout, State) ->
%%   lager:info("reset_timeout triggered"),
   {ok, State#state{values = []}};
handle_info(timeout, State = #state{timeout = T}) ->
%%   lager:info("timeout triggered"),
   NewTimer = start_timeout(T),
   {ok, State#state{values = [], timer = NewTimer}};
handle_info(_Req, State) ->
   {ok, State}.


-spec process_points(list(), list(), list(), list()) -> {list(), list()}.
process_points([], NewPoints, LastValues, _FieldNames) ->
   {NewPoints, LastValues};
process_points([P|RP], PointsAcc, LastValues, FieldNames) ->
   {NewFields, NewLastValues} = process_point(P, LastValues, FieldNames),
   process_points(RP, PointsAcc ++ [P#data_point{fields = NewFields}], NewLastValues, FieldNames).

-spec process_point(#data_point{}, list(), list()|undefined) -> {list(), list()}.
process_point(P=#data_point{fields = Fields}, [], undefined) ->
   {P, Fields};
process_point(#data_point{fields = Fields}, Fields, undefined) ->
   {#{}, Fields};
process_point(P=#data_point{fields = Fields}, _LastValue, undefined) ->
   {P, Fields};
process_point(Point = #data_point{}, [], FieldNames) ->
   {Point, get_values(Point, FieldNames)};
process_point(Point = #data_point{}, LastValues, FieldNames) ->
   NewValues = get_values(Point, FieldNames),
   Out = check(Point, LastValues, FieldNames, NewValues),
   {Out, NewValues}.


check(P, _, [], _) ->
   P;
check(P = #data_point{}, LastValues, [FName|FieldNames], NewValues) ->
   case proplists:get_value(FName, LastValues) of
      undefined -> check(P, LastValues, FieldNames, NewValues);
      Val -> case proplists:get_value(FName, NewValues) of
                undefined -> check(P, LastValues, FieldNames, NewValues);
                Value when Value =:= Val -> #{};
                _ -> check(P, LastValues, FieldNames, NewValues)
             end
   end.

-spec get_values(#data_point{}, list()) -> list({Key :: binary(), Val :: any()}).
get_values(P = #data_point{}, FieldNames) ->
   Values = flowdata:fields(P, FieldNames),
   Pairs = lists:zip(FieldNames, Values),
   lists:filter(fun({_N, E}) -> E /= undefined end, Pairs).


-spec reset_timeout(undefined|non_neg_integer()) -> undefined|reference().
reset_timeout(undefined) -> undefined;
reset_timeout(Time) ->
%%   lager:notice("start reset timeout"),
   erlang:send_after(Time, self(), reset_timeout).
start_timeout(undefined) -> undefined;
start_timeout(Time) ->
%%   lager:notice("start timeout"),
   erlang:send_after(Time, self(), timeout).


-spec cancel_timer(undefined|reference()) -> ok|non_neg_integer()|false.
cancel_timer(undefined) -> ok;
cancel_timer(TimerRef) when is_reference(TimerRef) ->
   erlang:cancel_timer(TimerRef, [{async, true}, {info, false}]).

timer_interval(undefined) -> undefined;
timer_interval(Duration) -> faxe_time:duration_to_ms(Duration).


-ifdef(TEST).
process_point_monitor_last_test() ->
   P = process_datapoint0(),
   LastVals = [{<<"val">>, 2.5}],
   ?assertEqual({P, [{<<"val">>, flowdata:field(P, <<"val">>)}]},
      process_point(P, LastVals, [<<"val">>])).
process_point_monitor_nolast_test() ->
   P = process_datapoint0(),
   LastVals = [],
   ?assertEqual({P, [{<<"val">>, flowdata:field(P, <<"val">>)}]},
      process_point(P, LastVals, [<<"val">>])).
process_point_monitor_lastequal_test() ->
   P = process_datapoint0(),
   LastVals = [{<<"val">>, flowdata:field(P, <<"val">>)}],
   NewVals = LastVals,
   ?assertEqual({#{}, NewVals}, process_point(P, LastVals, [<<"val">>])).
process_point_monitor_one_lastequal_test() ->
   P = process_datapoint0(),
   LastVals = [{<<"val">>, #{<<"me">> => <<"muu">>}},{<<"val1">>, 1.343}],
   NewVals = [{<<"val">>, flowdata:field(P, <<"val">>)},{<<"val1">>, flowdata:field(P, <<"val1">>)}],
   ?assertEqual({#{}, NewVals}, process_point(P, LastVals, [<<"val">>, <<"val1">>])).
process_point_all_nolast_test() ->
   P = process_datapoint(),
   LastVals = [],
   ?assertEqual({P, P#data_point.fields}, process_point(P, LastVals, undefined)).
process_point_all_last_equal_test() ->
   P = process_datapoint(),
   LastVals = P#data_point.fields,
   ?assertEqual({#{}, P#data_point.fields}, process_point(P, LastVals, undefined)).
process_point_all_last_nonequal_test() ->
   P = process_datapoint(),
   Fields = P#data_point.fields,
   LastVals0 = flowdata:set_field(P, <<"x.tails[3]">>, 2.5),
   LastVals = LastVals0#data_point.fields,
   ?assertEqual({P, Fields}, process_point(P, LastVals, undefined)).
process_point_filter_last_equal_test() ->
   P = process_datapoint(),
   LastVals = [{<<"x.tails">>, [1,2,3,4]}],
   ?assertEqual({#{}, [{<<"x.tails">>, flowdata:field(P, <<"x.tails">>)}]},
      process_point(P, LastVals, [<<"x.tails">>])).
process_point_filter_last_nonequal_test() ->
   P = process_datapoint(),
   LastVals = [{<<"x.tails[3]">>, 2.5}],
   ?assertEqual({P, [{<<"x.tails">>, flowdata:field(P, <<"x.tails">>)}]},
      process_point(P, LastVals, [<<"x.tails">>])).
process_datapoint0() ->
   #data_point{ts = 1, fields = maps:from_list([{<<"val">>, 1}, {<<"val1">>, 1.343}, {<<"val2">>, 2.222}])}.
process_datapoint() ->
   #data_point{ts = 1, fields = #{<<"x">> => #{<<"tails">> => [1,2,3,4],
      <<"head">> => #{<<"pitch">> => <<"noop">>}}, <<"y">> => 45.3} }.
-endif.