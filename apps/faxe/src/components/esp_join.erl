%% Date: 05.01.17 - 14:11
%% Ⓒ 2017 heyoka
%% @doc
%% Join Datapoints from two or more nodes
%% When considering the "fill" option, the following rules apply:
%% * none - (default) skip rows where a point is missing, inner join.
%% * null - fill missing points with null, full outer join.
%% * Any numerical value - fill fields with given value, full outer join.
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
   as,
   tolerance,
   granularity = {minute, 5}, %% if given, means: align the joining to the time-interval
   m_timeout,
   timers = []
}).


inports() ->
   [{1, infinity}].

options() -> [
   {joined, nodes, {ports, [2,3,4,5]}},
   {as,     string_list, [<<"val">>]},
   %% @todo maybe this should be aligned to wall clock ?
   %% a wall-clock timeout will be set up per time unit to collect all values,
   %% values that do not arrive within the given timeout will be treated as missing, in ms
   {missing_timeout, duration, <<"20s">>},
   %% timestamp tolerance in ms
   {tolerance, duration, <<"2s">>},
   %% missing-value handling
   {fill,   any,     none} %% none, null, any numerical value
].

init(NodeId, Ins, #{as := As, missing_timeout := MTimeOut, tolerance := Tol}) ->
   RowLength = length(Ins),
   As1 = lists:zip(lists:seq(1,RowLength), As),
   io:format("~p init:node :: ~p~n",[NodeId, Ins]),
   {ok, all,
      #state{node_id = NodeId, as = As1, row_length = length(Ins),
      tolerance = faxe_time:duration_to_ms(Tol),
      m_timeout = faxe_time:duration_to_ms(MTimeOut)}}.

process(Inport, #data_point{ts = Ts} = Point, State = #state{buffer = undefined, m_timeout = M, timers = TList}) ->
   Buffer = orddict:new(),
   NewList = new_timeout(M, Ts, TList),
   lager:warning("new buffer entry for Ts: ~p : ~p",[Ts, {Inport, Point}]),
   NewBuffer = orddict:store(Ts, [{Inport, Point}], Buffer),
   {ok, State#state{buffer = NewBuffer, timers = NewList}};
process(Inport, #data_point{ts = Ts} = Point, State = #state{buffer = Buffer, timers = TList,
   as = As, m_timeout = MTimeout, row_length = RowLength, tolerance = Tolerance}) ->
   lager:notice("In on port : ~p",[Inport]),
   TsList = orddict:fetch_keys(Buffer),
%%   lager:info("new TsList in Buffer: ~p",[TsList]),
   LookupTs = nearest_ts(Ts, TsList),

   {NewTs, NewRow} =
   case abs(LookupTs - Ts) > Tolerance of
      true ->
         %% new ts in buffer
         lager:warning("new buffer entry for Ts: ~p : ~p",[Ts, {Inport, Point}]),
         {Ts , [{Inport, Point}]};
      false ->
         %% add point to buffer
         lager:warning("add buffer entry for Ts: ~p: ~p",[LookupTs, {Inport, Point}]),
         {LookupTs, [{Inport, Point}| orddict:fetch(LookupTs, Buffer)]}
   end,

   {NewBuffer, NewTimerList} =
   case length(NewRow) == RowLength of
      true ->
         dataflow:emit(join(NewRow, NewTs, As)),
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

%%   TimeNext = esp_time:beginning_unit(Unit, Ts),
%%   TimeBase = esp_time:add_unit(TimeNext, Unit, GVal),
%%   TimeBase1 = esp_time:align(G, Ts),
%%   Val = flowdata:field(Point, F),


   {ok, State#state{buffer = NewBuffer, timers = NewTimerList}}.

%% missing timeout
handle_info({tick, Ts}, State = #state{buffer = [], timers = TList}) ->
   lager:warning("~p empty buffer when ticked",[?MODULE]),
   {ok, State#state{timers = proplists:delete(Ts, TList), buffer = undefined}};
handle_info({tick, Ts}, State = #state{buffer = Buffer, as = As, timers = TList}) ->
   lager:warning("~p ticks missing timeout :: ~p",[?MODULE, Buffer]),
   NewBuffer =
   case lists:member(Ts, orddict:fetch_keys(Buffer)) of
      true ->
            {Row, NewDict} = orddict:take(Ts, Buffer),
            dataflow:emit(join(Row, Ts, As)),
            NewDict;
      false -> Buffer
   end,
%%   Row = orddict:fetch(Ts, Buffer),
%%   dataflow:emit(join(Row, Ts, As)),
%%   NewBuffer = orddict:erase(Ts, Buffer),
   {ok, State#state{buffer = NewBuffer, timers = proplists:delete(Ts, TList)}}.

%% actually join the buffer rows
join(RowData, Ts, As) ->
   NewPoint =
   lists:foldl(
      fun({Port, #data_point{fields = Fields}}, P=#data_point{fields = ResFields}) ->
         Prefix = proplists:get_value(Port, As),
         NewFields = [{<<Prefix/binary, <<".">>/binary, FName/binary>>, Val} || {FName, Val} <- Fields],
         P#data_point{fields = ResFields ++ NewFields}
      end,
      #data_point{ts = Ts},
      RowData
   ),
   lager:info("join OUT at ~p: ~p",[faxe_time:to_date(Ts),NewPoint]),
   NewPoint.

%% get the nearest from a list of values
nearest_ts(Ts, TsList) ->
   lists:foldl(
      fun(E, Nearest) ->
         case abs(Ts - E) < abs(Ts - Nearest) of
            true -> E;
            false -> Nearest
         end
      end, 0, TsList).

%% get the nearest from a list of values, which is less than the given value
nearest_ts_lower(Ts, TsList) ->
   NList = lists:filter(fun(ETs) -> (Ts - ETs) >= 0 end, TsList),
   nearest_ts(Ts, NList).

nearest_ts_higher(Ts, TsList) ->
   NList = lists:filter(fun(ETs) -> (Ts - ETs) =< 0 end, TsList),
   nearest_ts(Ts, NList).

new_timeout(T, Ts, TimerList) ->
   TRef = erlang:send_after(T, self(), {tick, Ts}),
   [{Ts, TRef}|TimerList].

clear_timeout(Ts, TimerList) ->
   case proplists:get_value(Ts, TimerList) of
      Ref when is_reference(Ref) ->
         lager:warning("clear timeout for ~p",[Ts]),
         catch(erlang:cancel_timer(Ref)),
         proplists:delete(Ts, TimerList);
      undefined -> TimerList
   end
   .

-ifdef(TEST).
%%   basic_test() -> ?assertEqual(16.6, execute([1,3,8,16,55], #{field => <<"val">>})).
-endif.