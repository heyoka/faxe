%% Date: 06.06 - 19:04
%% Ⓒ 2019 heyoka
%% @doc emits new point-values only if different from the previous point
%% * normal (non-exclusive) behaviour is: the node emits every value that is either not in the fields list or it
%% has changed
%%
%% * multiple fields can be monitored by this node, if no field is specified, the whole datapoint will be used for
%% comparison, here the exclusive parameter is useless
%%
%% * if reset_timeout is given, all previous values are reset, if there are no points
%% coming in for this amount of time
%%
%% * with the exclusive flag set, every given monitor field has to have a
%% changed value in order for the node to emit anything
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
   timer
}).

options() -> [
   {fields, binary_list, undefined},
   {reset_timeout, duration, <<"3h">>}
].

init(_NodeId, _Ins, #{fields := FieldList, reset_timeout := Timeout}) ->
   Time = faxe_time:duration_to_ms(Timeout),
   {ok, all, #state{fields = FieldList, reset_timeout = Time}}.

process(_In, #data_batch{points = Points} = Batch,
    State = #state{fields = FieldNames, values = Vals, timer = TRef, reset_timeout = Time}) ->
   cancel_timer(TRef),
   {NewPoints, LastValues} = process_points(Points, [], Vals, FieldNames),
   NewState = State#state{values = LastValues, timer = reset_timeout(Time)},
   case NewPoints of
      [] -> {ok, NewState};
      Es when is_list(Es) -> {emit, Batch#data_batch{points = NewPoints}}
   end;
process(_Inport, #data_point{} = Point,
    State = #state{fields = Fields, values = LastValues, timer = TRef, reset_timeout = Time}) ->
%%   lager:notice("~p point in : ~p", [?MODULE, Point#data_point.fields]),
   cancel_timer(TRef),
   {Filtered, NewValues} = process_point(Point, LastValues, Fields),
%%   lager:info("new last values: ~p~nFiltered: ~p" ,[NewValues, Filtered]),
   NewState = State#state{values = NewValues, timer = reset_timeout(Time)},
   case Filtered of
      Map when is_map(Map), map_size(Map) == 0 ->  {ok, NewState};
      E when is_map(E) -> %lager:notice("~p emitting: ~p", [?MODULE, Filtered]),
         {emit, Point#data_point{fields = Filtered}, NewState}
   end
   .


handle_info(reset_timeout, State) ->
   {ok, State#state{values = []}};
handle_info(_Req, State) ->
   {ok, State}.


-spec process_points(list(), list(), list(), list()) -> {list(), list()}.
process_points([], NewPoints, LastValues, _FieldNames) ->
   {NewPoints, LastValues};
process_points([P|RP], PointsAcc, LastValues, FieldNames) ->
   {NewFields, NewLastValues} = process_point(P, LastValues, FieldNames),
   process_points(RP, PointsAcc ++ [P#data_point{fields = NewFields}], NewLastValues, FieldNames).

-spec process_point(#data_point{}, list(), list()|undefined) -> {list(), list()}.
process_point(#data_point{fields = Fields}, [], undefined) ->
   {Fields, Fields};
process_point(#data_point{fields = Fields}, Fields, undefined) ->
   {#{}, Fields};
process_point(#data_point{fields = Fields}, _LastValue, undefined) ->
   {Fields, Fields};
process_point(Point = #data_point{fields = Fields}, [], FieldNames) ->
   {Fields, get_values(Point, FieldNames)};
process_point(Point = #data_point{}, LastValues, FieldNames) ->
   Out =
   case check(Point, LastValues, FieldNames) of
      true -> Point#data_point.fields;
      false -> #{}
   end,
%%   Filtered = filter(Point, LastValues),
   {Out, get_values(Point,FieldNames)}.


check(_, _, []) ->
   true;
check(P = #data_point{}, LastValues, [FName|FieldNames]) ->
   case proplists:get_value(FName, LastValues) of
      undefined -> check(P, LastValues, FieldNames);
      Val -> case flowdata:field(P, FName) of
                undefined -> check(P, LastValues, FieldNames);
                Value when Value =:= Val -> false;
                _ -> check(P, LastValues, FieldNames)
             end
   end.

-spec get_values(#data_point{}, list()) -> list({Key :: binary(), Val :: any()}).
get_values(P = #data_point{}, FieldNames) ->
   lists:filter(fun(E) -> E /= undefined end, [{Field, flowdata:field(P, Field)} || Field <- FieldNames]).


-spec reset_timeout(undefined|non_neg_integer()) -> undefined|reference().
reset_timeout(undefined) -> undefined;
reset_timeout(Time) -> erlang:send_after(Time, self(), reset_timeout).

-spec cancel_timer(undefined|reference()) -> ok|non_neg_integer()|false.
cancel_timer(undefined) -> ok;
cancel_timer(TimerRef) when is_reference(TimerRef) ->
   erlang:cancel_timer(TimerRef, [{async, true}, {info, false}]).




-ifdef(TEST).
process_point_monitor_last_test() ->
   P = process_datapoint0(),
   LastVals = [{<<"val">>, 2.5}],
   ?assertEqual({P#data_point.fields, [{<<"val">>, flowdata:field(P, <<"val">>)}]},
      process_point(P, LastVals, [<<"val">>])).
process_point_all_nolast_test() ->
   P = process_datapoint(),
   LastVals = [],
   ?assertEqual({P#data_point.fields, P#data_point.fields}, process_point(P, LastVals, undefined)).
process_point_all_last_equal_test() ->
   P = process_datapoint(),
   LastVals = P#data_point.fields,
   ?assertEqual({#{}, P#data_point.fields}, process_point(P, LastVals, undefined)).
process_point_all_last_nonequal_test() ->
   P = process_datapoint(),
   Fields = P#data_point.fields,
   LastVals0 = flowdata:set_field(P, <<"x.tails[3]">>, 2.5),
   LastVals = LastVals0#data_point.fields,
   ?assertEqual({Fields, Fields}, process_point(P, LastVals, undefined)).
process_point_filter_last_equal_test() ->
   P = process_datapoint(),
   LastVals = [{<<"x.tails">>, [1,2,3,4]}],
   ?assertEqual({#{}, [{<<"x.tails">>, flowdata:field(P, <<"x.tails">>)}]},
      process_point(P, LastVals, [<<"x.tails">>])).
process_point_filter_last_nonequal_test() ->
   P = process_datapoint(),
   Fields = P#data_point.fields,
   LastVals = [{<<"x.tails[3]">>, 2.5}],
   ?assertEqual({Fields, [{<<"x.tails">>, flowdata:field(P, <<"x.tails">>)}]},
      process_point(P, LastVals, [<<"x.tails">>])).
process_datapoint0() ->
   #data_point{ts = 1, fields = maps:from_list([{<<"val">>, 1}, {<<"val1">>, 1.343}, {<<"val2">>, 2.222}])}.
process_datapoint() ->
   #data_point{ts = 1, fields = #{<<"x">> => #{<<"tails">> => [1,2,3,4],
      <<"head">> => #{<<"pitch">> => <<"noop">>}}, <<"y">> => 45.3} }.
-endif.