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

%% @end
-module(esp_join2).
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
   buffer = [],
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
   %% values that do not arrive within the given timeout will be treated as missing
   {missing_timeout, duration, <<"20s">>},
   %% timestamp tolerance in ms
   {tolerance, duration, <<"2s">>},
   %% missing-value handling
   {full, boolean, true} %% replaces 'fill'
].

init(NodeId, Ins,
    #{
       prefix := Prefix, missing_timeout := MTimeOut, full := Full,
       tolerance := Tol, merge_field := FMerge} = Opts) ->


   RowList = proplists:get_keys(Ins),
   RowLength = length(Ins),
   Prefix1 =
   case FMerge of
      undefined -> lists:zip(lists:seq(1, RowLength), Prefix);
      _ -> nil
   end,
   lager:notice("Opts: ~p",[Opts]),
   {ok, all,
      #state{node_id = NodeId, prefix = Prefix1, field_merge = FMerge, row_length = RowLength,
      tolerance = faxe_time:duration_to_ms(Tol), row_list = RowList,
      m_timeout = faxe_time:duration_to_ms(MTimeOut), full = Full}}.

process(Inport, #data_point{ts = Ts} = Point, State = #state{buffer = [], m_timeout = M, timers = TList}) ->
%%   lager:warning("+++ buffer empty ~p",[Ts]),
   RowId = 1,
   NewList = new_timeout(M, {Ts, RowId}, TList),
   NewBuffer = [{Ts, [{RowId, [{Inport, Point}]}]}],
   {ok, State#state{buffer = NewBuffer, timers = NewList}};
process(Inport, #data_point{ts = Ts} = Point, State = #state{buffer = Buffer, timers = TimerList,
      m_timeout = MTimeout, tolerance = Tolerance}) ->

%%   {message_queue_len, MsgQueueLength} = erlang:process_info(self(), message_queue_len),
%%   case MsgQueueLength > 50 of
%%      true -> lager:alert("message_queue_len above 50 -> ~p",[MsgQueueLength]);
%%      false -> ok
%%   end,
%%   lager:info("timers ~p",[TimerList]),

   {_T, Result} = timer:tc(fun do_process/3, [Inport, Point, State]),
%%   lager:alert("processed in ~p my",[T]),
%%   lager:warning("NewBuffer: ~p ~n NewTsList: ~p",[NewBuffer, NewTimerList]),
   Result.
%%   {ok, State#state{buffer = NewBuffer, timers = NewTimerList}}.


do_process(Inport, #data_point{ts = Ts} = Point, State = #state{buffer = Buffer, timers = TimerList,
   m_timeout = MTimeout, tolerance = Tolerance}) ->
   %% unique list of keys
   TsList = proplists:get_keys(Buffer),
   LookupTs = nearest_ts(Ts, TsList),

%%   lager:info("~p :: ts-list: ~p nearest: ~p, DIFF ~p",[Ts, TsList, LookupTs, abs(LookupTs - Ts)]),

   {NewBuffer, NewTimerList} =
      case abs(LookupTs - Ts) > Tolerance of
         true ->
            %% new ts in buffer
            RowId = 1,
            NewTimers = new_timeout(MTimeout, {Ts, RowId}, TimerList),
            {Buffer ++ [{Ts, [{RowId, [{Inport, Point}]}]}], NewTimers};
         false ->
            %% add/append point to buffer
            [All] = proplists:get_all_values(LookupTs, Buffer),
            NotFindFun =
               fun
               %% we have already found an entry, done appending
                  ({_Id, _Row}, {true, _Ret, _TList} = R) -> R;
                  %% iteration, still work to do
                  ({Id, Row}, {false, _, TList} = R1) ->
                     case proplists:lookup(Inport, Row) of
                        %% we have found a row, that has not got a value from this port already, so we add our entry here
                        none ->
                           AllNew = lists:keydelete(Id, 1, All),
                           %% build a new row first
                           NewRow = Row ++ [{Inport, Point}],
                           %% check if we got a full row with this already
                           case is_full_row(NewRow, State) of
                              %% yes full row, so emit it and clear a running timeout and return row-list with already deleted row
                              true ->
                                 do_emit(NewRow, LookupTs, State),
                                 NewTimerListInner = clear_timeout({LookupTs, Id}, TList),
                                 {true, AllNew, NewTimerListInner};
                              %% no not a full row yet, so actually add the new row to the row-list
                              false ->
                                 {true, lists:keysort(1, AllNew ++ [{Id, NewRow}]), TList}
                           end;
                        %% nope, this row has an entry from our port already
                        _V -> R1
                     end
               end,
            FRes = lists:foldl(NotFindFun, {false, All, TimerList}, All),
%%         lager:info("FRES ~p ",[FRes]),
            Res =
               case FRes of
                  %% no row was found, to add the entry to, so start a new row for this Key
                  {false, _AllAsIs, RTimerList} ->
                     NewId = lists:max(proplists:get_keys(All)) + 1,
%%                     lager:info("New RowId ~p for ~p",[NewId, LookupTs]),
                     NewAll = All ++ [{NewId, [{Inport, Point}]}],
                     %% start a timeout for the newly added row
                     NewTimerListRes = new_timeout(200, {LookupTs, NewId}, RTimerList),
                     {proplists:delete(LookupTs, Buffer) ++ [{LookupTs, NewAll}], NewTimerListRes};
                  %% we added the new entry to an existing row
                  {true, [], RTimerList} -> {proplists:delete(LookupTs, Buffer), RTimerList};
                  {true, NewAll, RTimerList} -> {proplists:delete(LookupTs, Buffer) ++ [{LookupTs, NewAll}], RTimerList}
               end,
%%         lager:notice("BAG: ~n~p~nResult: ~n~p",[Buffer, Res]),
            Res
%%         lager:info("incoming point (~p) added to: ~p",[Point, {LookupTs, Inport}]),
%%         {LookupTs, [{Inport, Point}| orddict:fetch(LookupTs, Buffer)]}
      end,
%%   lager:warning("NewBuffer: ~p ~n NewTsList: ~p",[NewBuffer, NewTimerList]),
   {ok, State#state{buffer = NewBuffer, timers = NewTimerList}}.


%% missing timeout
handle_info({tick, TsKey}, State = #state{buffer = [], timers = TList}) ->
   {ok, State#state{timers = proplists:delete(TsKey, TList), buffer = undefined}};
handle_info({tick, {Ts, Id} = TsKey}, State = #state{buffer = Buffer, timers = TList}) ->
%%   lager:info("tick ~p",[TsKey]),
   NewBuffer =
   case take(Ts, Id, Buffer) of
      undefined ->
         Buffer;
      {Row, Buffer1} ->
         maybe_emit(Row, Ts, State),
         Buffer1
   end,
   {ok, State#state{buffer = NewBuffer, timers = proplists:delete(TsKey, TList)}};
handle_info(_R, State) ->
   {ok, State}.


maybe_emit(NewRow, NewTs, State = #state{full = true}) ->
%%   lager:info("maybe_emit [fill=none]: ~p",[{NewRow, NewTs}]),
   case is_full_row(NewRow, State) of
      true -> do_emit(NewRow, NewTs, State);
      false -> lager:warning("row is not full yet, will drop item"),
         ok
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

new_timeout(T, {_Ts, _Id} = TsKey, TimerList) ->
   TRef = erlang:send_after(T, self(), {tick, TsKey}),
   [{TsKey, TRef}|TimerList].

clear_timeout({_Ts, _RId} = TsKey, TimerList) ->
   case proplists:get_value(TsKey, TimerList) of
      Ref when is_reference(Ref) ->
         catch(erlang:cancel_timer(Ref)),
         proplists:delete(TsKey, TimerList);
      undefined -> TimerList
   end
   .

%% take a row of data from the Buffer structure
-spec take(non_neg_integer(), non_neg_integer(), list()) -> undefined|{ResultRow::list(), NewBuffer::list()}.
take(Key, SubKey, Bag) ->
   case proplists:get_value(Key, Bag) of
      undefined -> undefined;
      Rows ->
         case lists:keytake(SubKey, 1, Rows) of
            {value, {SubKey, ResultRow}, Left0} ->
               Left = case Left0 of [] -> []; _ -> [{Key, Left0}] end,
               {ResultRow, proplists:delete(Key, Bag) ++ Left};
            false ->
               undefined
         end
   end.

fill(<<"none">>) -> false;
fill(none) -> false;
fill(false) -> false;
fill(_) -> true.

is_full_row(Row, #state{row_list = RowList, row_length = _RowLen}) ->
   case RowList -- proplists:get_keys(Row) of
      [] -> true;
      Left  -> false
   end.

merge(M1, M2) when is_map(M1), is_map(M2) -> mapz:deep_merge(merge_fun(), #{}, [M1, M2]);
merge(M1, M2) when is_list(M1), is_list(M2) -> lists:merge(M1, M2);
merge(V1, V2) when is_number(V1), is_number(V2) -> V1 + V2;
merge(S1, S2) when is_binary(S1), is_binary(S2) -> string:concat(S1, S2);
merge(A, B) ->
   error( {iolist_to_binary(["cannot merge ",
      faxe_util:type(B),  " into ",
      faxe_util:type(A)]),
      B, A
   }).


merge_fun() ->
   fun
      (Prev, Val) when is_map(Prev), is_map(Val) -> maps:merge(Prev, Val);
      (Prev, Val) when is_list(Prev), is_list(Val) -> lists:merge(Prev, Val);
      (_, Val) -> Val
   end.