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
%% @end
-module(esp_join).
-author("Alexander Minichmair").

%% API
-behavior(df_component).

-include("faxe.hrl").
%% API
-export([init/3, process/3, options/0, handle_info/2, inports/0]).


-record(state, {
   node_id,
   row_length :: non_neg_integer(),
   lookup = [] :: list(), %% buffer-keys
   buffer, %% orddict with timstamps as keys
   prefix,
   field_merge,
   tolerance,
   %granularity = {minute, 5}, %% if given, means: align the joining to the time-interval
   m_timeout,
   timers = []
}).


inports() ->
   [{1, infinity}].

options() -> [
   {joined, nodes, {ports, [2,3,4,5]}},
   {prefix,     string_list, [<<"val1">>, <<"val2">>]},
   {field_merge, string, undefined},
   %% @todo maybe this should be aligned to wall clock ?
   %% a wall-clock timeout will be set up per time unit to collect all values,
   %% values that do not arrive within the given timeout will be treated as missing, in ms
   {missing_timeout, duration, <<"20s">>},
   %% timestamp tolerance in ms
   {tolerance, duration, <<"2s">>},
   %% missing-value handling
   {fill,   any,     none} %% none, null, any numerical value
].

init(NodeId, Ins,
    #{prefix := Prefix, missing_timeout := MTimeOut, tolerance := Tol, field_merge := FMerge}) ->
   RowLength = length(Ins),
   Prefix1 =
   case FMerge of
      undefined -> lists:zip(lists:seq(1,RowLength), Prefix);
      _ -> nil
   end,

   lager:notice("~p init:node :: ~p~n",[NodeId, Ins]),
   {ok, all,
      #state{node_id = NodeId, prefix = Prefix1, field_merge = FMerge, row_length = length(Ins),
      tolerance = faxe_time:duration_to_ms(Tol),
      m_timeout = faxe_time:duration_to_ms(MTimeOut)}}.

process(Inport, #data_point{ts = Ts} = Point, State = #state{buffer = undefined, m_timeout = M, timers = TList}) ->
   Buffer = orddict:new(),
   NewList = new_timeout(M, Ts, TList),
%%   lager:warning("new buffer entry for Ts: ~p : ~p",[Ts, {Inport, Point}]),
   NewBuffer = orddict:store(Ts, [{Inport, Point}], Buffer),
   {ok, State#state{buffer = NewBuffer, timers = NewList}};
process(Inport, #data_point{ts = Ts} = Point, State = #state{buffer = Buffer, timers = TList,
   prefix = As, field_merge = FMerge, m_timeout = MTimeout, row_length = RowLength, tolerance = Tolerance}) ->
%%   lager:notice("In on port : ~p",[Inport]),
   TsList = orddict:fetch_keys(Buffer),
%%   lager:info("new TsList in Buffer: ~p",[TsList]),
   LookupTs = nearest_ts(Ts, TsList),

   {NewTs, NewRow} =
   case abs(LookupTs - Ts) > Tolerance of
      true ->
         %% new ts in buffer
%%         lager:warning("new buffer entry for Ts: ~p : ~p",[Ts, {Inport, Point}]),
         {Ts , [{Inport, Point}]};
      false ->
         %% add point to buffer
%%         lager:warning("add buffer entry for Ts: ~p: ~p",[LookupTs, {Inport, Point}]),
         {LookupTs, [{Inport, Point}| orddict:fetch(LookupTs, Buffer)]}
   end,

   {NewBuffer, NewTimerList} =
   case length(NewRow) == RowLength of
      true ->
         dataflow:emit(conflate(NewRow, NewTs, As, FMerge)),
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
%%   lager:warning("~p empty buffer when ticked",[?MODULE]),
   {ok, State#state{timers = proplists:delete(Ts, TList), buffer = undefined}};
handle_info({tick, Ts},
    State = #state{buffer = Buffer, prefix = As, field_merge = FMerge, timers = TList}) ->
%%   lager:warning("~p ticks missing timeout :: ~p",[?MODULE, Buffer]),
   NewBuffer =
   case lists:member(Ts, orddict:fetch_keys(Buffer)) of
      true ->
            {Row, NewDict} = orddict:take(Ts, Buffer),
            dataflow:emit(conflate(Row, Ts, As, FMerge)),
            NewDict;
      false -> Buffer
   end,
   {ok, State#state{buffer = NewBuffer, timers = proplists:delete(Ts, TList)}}.

%% conflate the rows in buffer
conflate(RowData, Ts, Prefixes, undefined) ->
   join(RowData, Ts, Prefixes);
conflate(RowData, Ts, _Prefixes, FieldMerge) ->
   merge(RowData, Ts, FieldMerge).

merge(RowData, Ts, MField) ->
   MergeField = flowdata:path(MField),
   lager:notice("MERGE ROWS: ~p~n", [length(RowData)]),
   [lager:notice("~p~n", [Port]) || {Port, _Row} <- RowData],
   NewPoint =
      lists:foldl(
         fun({_Port, OP=#data_point{}}, P=#data_point{}) ->
            M1 = flowdata:field(OP, MergeField), M2 = flowdata:field(P, MergeField),
            NewData = maps:merge(M1, M2),
            flowdata:set_field(P, MergeField, NewData)
%%            P#data_point{fields = #{MergeField => NewData}}
         end,
         flowdata:set_field(#data_point{ts = Ts}, MergeField, #{}),
         RowData
      ),
%%   lager:info("join OUT at ~p: ~p",[faxe_time:to_date(Ts),NewPoint]),
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
%%   lager:info("join OUT at ~p: ~p",[faxe_time:to_date(Ts),NewPoint]),
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

%% get the nearest from a list of values, which is less than the given value
%%nearest_ts_lower(Ts, TsList) ->
%%   NList = lists:filter(fun(ETs) -> (Ts - ETs) >= 0 end, TsList),
%%   nearest_ts(Ts, NList).
%%
%%nearest_ts_higher(Ts, TsList) ->
%%   NList = lists:filter(fun(ETs) -> (Ts - ETs) =< 0 end, TsList),
%%   nearest_ts(Ts, NList).

new_timeout(T, Ts, TimerList) ->
   TRef = erlang:send_after(T, self(), {tick, Ts}),
   [{Ts, TRef}|TimerList].

clear_timeout(Ts, TimerList) ->
   case proplists:get_value(Ts, TimerList) of
      Ref when is_reference(Ref) ->
%%         lager:warning("clear timeout for ~p",[Ts]),
         catch(erlang:cancel_timer(Ref)),
         proplists:delete(Ts, TimerList);
      undefined -> TimerList
   end
   .

-ifdef(TEST).
%%   basic_test() -> ?assertEqual(16.6, execute([1,3,8,16,55], #{field => <<"val">>})).
-endif.