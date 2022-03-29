%% Date: 05.01.17 - 14:11
%% Ⓒ 2017 heyoka
%% @doc
%% Join Datapoints from two or more nodes, given a list of prefixes, for each row
%% If the 'field_merge' parameter is given, the node will merge the field given from every in-node, instead of
%% joining.
%% When considering the "fill" option, the following rules apply:
%% * none - (default) skip rows where a point is missing, inner join.
%% * null - fill missing points with null, full outer join.
%% * Any numerical value - fill fields with given value, full outer join.
%% Note, that this node will produce a completely new value.
%%
%% Note, with field_merge, no deep merge is performed, also, if the fields to merge contain different data-types
%% merging is not possible
%% data-types that can be merged: 2 maps, 2 lists, 2 numbers (which will be added), strings (will be concatenated)
%% @todo make sure points that are handled are coming in on different ports (missing timeout)
%% @end
-module(esp_join).
-author("Alexander Minichmair").

%% API
-behavior(df_component).

-include("faxe.hrl").
%% API
-export([init/3, process/3, options/0, handle_info/2, inports/0, wants/0, emits/0]).


-record(state, {
   node_id,
   row_length :: non_neg_integer(),
   row_list :: list(),
   lookup = [] :: list(), %% buffer-keys
   buffer, %% orddict with timstamps as keys
   prefix,
   full,
   field_merge,
   tolerance,
   m_timeout,
   timers = []
}).


wants() -> point.
emits() -> point.

inports() ->
   [{1, infinity}].

options() -> [
   {joined, nodes, {ports, [2,3,4,5]}},
   {prefix,     string_list, [<<"">>, <<"">>]},
   {merge_field, string, undefined},
   %% @todo maybe this should be aligned to wall clock ?
   %% a wall-clock timeout will be set up per time unit to collect all values,
   %% values that do not arrive within the given timeout will be treated as missing, in ms
   {missing_timeout, duration, <<"20s">>},
   %% timestamp tolerance in ms
   {tolerance, duration, <<"2s">>},
   %% missing-value handling
   {fill,   any,     none}, %% deprecated, use 'full' instead
   {full, boolean, true} %% replaces 'fill'
].

init(NodeId, Ins,
    #{
       prefix := Prefix, missing_timeout := MTimeOut, full := Full,
       tolerance := Tol, merge_field := FMerge, fill := Fill}) ->

   FullFill = case Full of false -> false; true -> not fill(Fill) end,
   lager:info("fullfill ~p",[FullFill]),
   RowList = proplists:get_keys(Ins),
   RowLength = length(Ins),
   Prefix1 =
   case FMerge of
      undefined -> lists:zip(lists:seq(1, RowLength), Prefix);
      _ -> nil
   end,

   {ok, all,
      #state{node_id = NodeId, prefix = Prefix1, field_merge = FMerge, row_length = RowLength,
      tolerance = faxe_time:duration_to_ms(Tol), row_list = RowList,
      m_timeout = faxe_time:duration_to_ms(MTimeOut), full = FullFill}}.

process(Inport, #data_point{ts = Ts} = Point, State = #state{buffer = undefined, m_timeout = M, timers = TList}) ->
   Buffer = orddict:new(),
   NewList = new_timeout(M, Ts, TList),
   NewBuffer = orddict:store(Ts, [{Inport, Point}], Buffer),
   {ok, State#state{buffer = NewBuffer, timers = NewList}};
process(Inport, #data_point{ts = Ts} = Point, State = #state{buffer = Buffer, timers = TList,
      m_timeout = MTimeout, tolerance = Tolerance}) ->

   TsList = orddict:fetch_keys(Buffer),
   LookupTs = nearest_ts(Ts, TsList),

   {NewTs, NewRow} =
   case abs(LookupTs - Ts) > Tolerance of
      true ->
         %% new ts in buffer
%%         lager:info("incoming point (~p) resulting in new row: ~p",[Point, {Ts, Inport}]),
         {Ts , [{Inport, Point}]};
      false ->
         %% add point to buffer
%%         lager:info("incoming point (~p) added to: ~p",[Point, {LookupTs, Inport}]),
         {LookupTs, [{Inport, Point}| orddict:fetch(LookupTs, Buffer)]}
   end,



   {NewBuffer, NewTimerList} =
   case is_full_row(NewRow, State) of
      true ->
         do_emit(NewRow, NewTs, State),
         {orddict:erase(NewTs, Buffer), clear_timeout(NewTs, TList)};

      false ->
         NBuffer = orddict:store(NewTs, NewRow, Buffer),
         case length(NewRow) of
            1 ->
               %% send with timeout, when a new row has been written before
               NewList = new_timeout(MTimeout, NewTs, TList),
               {NBuffer, NewList};
            _ ->
               {NBuffer, TList}
         end

   end,
   {ok, State#state{buffer = NewBuffer, timers = NewTimerList}}.

%% missing timeout
handle_info({tick, Ts}, State = #state{buffer = [], timers = TList}) ->
   {ok, State#state{timers = proplists:delete(Ts, TList), buffer = undefined}};
handle_info({tick, Ts}, State = #state{buffer = Buffer, timers = TList}) ->
   NewBuffer =
   case lists:member(Ts, orddict:fetch_keys(Buffer)) of
      true ->
            {Row, NewDict} = orddict:take(Ts, Buffer),
            maybe_emit(Row, Ts, State),
            NewDict;
      false -> Buffer
   end,
   {ok, State#state{buffer = NewBuffer, timers = proplists:delete(Ts, TList)}};
handle_info(_R, State) ->
   {ok, State}.


maybe_emit(NewRow, NewTs, State = #state{full = true}) ->
%%   lager:info("maybe_emit [fill=none]: ~p",[{NewRow, NewTs}]),
   case is_full_row(NewRow, State) of
      true -> do_emit(NewRow, NewTs, State);
      false -> lager:warning("row not filled"), ok
   end;
maybe_emit(NewRow, NewTs, State = #state{}) ->
%%   lager:info("maybe_emit: ~p",[{NewRow, NewTs}]),
   do_emit(NewRow, NewTs, State).

do_emit(Row, Ts, #state{prefix = Pref, field_merge = FMerge}) ->
   dataflow:emit(conflate(Row, Ts, Pref, FMerge)).

%% conflate the rows in buffer
conflate(RowData, Ts, Prefixes, undefined) ->
   join(RowData, Ts, Prefixes);
conflate(RowData, Ts, _Prefixes, FieldMerge) ->
   merge(RowData, Ts, FieldMerge).

merge([{_Port, RowData}], _Ts, _MField) -> RowData;
merge([{_Port, FirstRow}|RowData], Ts, MergeField) ->
   NewPoint =
      lists:foldl(
         fun({_Port0, OP=#data_point{}}, P=#data_point{}) ->
            M1 = flowdata:field(OP, MergeField), M2 = flowdata:field(P, MergeField),
%%            lager:notice("merge: ~p ~nand ~p",[M1, M2]),
            NewData = merge(M1, M2),
            flowdata:set_field(P, MergeField, NewData)
         end,
         FirstRow#data_point{ts = Ts},
         RowData
      ),
   NewPoint.

%% actually join the buffer rows
join(RowData, Ts, Prefixes) ->
   NewPoint =
   lists:foldl(
      fun({Port, #data_point{fields = Fields}}, P=#data_point{}) ->
         Prefix = proplists:get_value(Port, Prefixes),
         NewFields =
            [{<<Prefix/binary, FName/binary>>, Val} ||
               {FName, Val} <- maps:to_list(Fields)],
         flowdata:set_fields(P, NewFields)
      end,
      #data_point{ts = Ts},
      RowData
   ),
   NewPoint.

%% get the nearest from a list of values
%% the absolute value difference is used here
nearest_ts(Ts, TsList) ->
   lists:foldl(
      fun(E, Nearest) ->
         case abs(Ts - E) < abs(Ts - Nearest) of
            true -> E;
            false -> Nearest
         end
      end, 0, TsList).

new_timeout(T, Ts, TimerList) ->
   TRef = erlang:send_after(T, self(), {tick, Ts}),
   [{Ts, TRef}|TimerList].

clear_timeout(Ts, TimerList) ->
   case proplists:get_value(Ts, TimerList) of
      Ref when is_reference(Ref) ->
         catch(erlang:cancel_timer(Ref)),
         proplists:delete(Ts, TimerList);
      undefined -> TimerList
   end
   .

fill(<<"none">>) -> false;
fill(none) -> false;
fill(false) -> false;
fill(_) -> true.

is_full_row(Row, #state{row_list = RowList, row_length = _RowLen}) ->
   case RowList -- proplists:get_keys(Row) of
      [] -> true;
      _  -> false
   end.

merge(M1, M2) when is_map(M1), is_map(M2) -> mapz:deep_merge(merge_fun(), #{}, [M1, M2]);
merge(M1, M2) when is_list(M1), is_list(M2) -> lists:merge(M1, M2);
merge(V1, V2) when is_number(V1), is_number(V2) -> V1 + V2;
merge(S1, S2) when is_binary(S1), is_binary(S2) -> string:concat(S1, S2);
merge(_, _) -> error(cannot_merge_wrong_or_different_datatypes).


merge_fun() ->
   fun
      (Prev, Val) when is_map(Prev), is_map(Val) -> maps:merge(Prev, Val);
      (Prev, Val) when is_list(Prev), is_list(Val) -> lists:merge(Prev, Val);
      (_, Val) -> Val
   end.