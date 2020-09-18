%%%-------------------------------------------------------------------
%%% @author heyoka
%%% @copyright (C) 2019, <COMPANY>
%%% @doc
%%%
%%% pull data via modbus tcp, supported read functions are :
%%% ['coils', 'hregs', 'iregs', 'inputs', 'memory']
%%%
%%% read multiple values with possibly different functions at once
%%%
%%% parameters are:
%%% + ip
%%% + port, defaults to 502
%%% + every(duration) interval for reading values from a modbus device, defaults to 1s
%%% + device(integer) the modbus device id
%%% // mandatory read params:
%%% + function(string_list) a list of read functions
%%% + from(integer_list) a list of start values to read
%%% + count(integer_list) a list of count values -> how much to read for each function
%%% + as(string_list) a list of fieldnames to use for the retrieved values
%%%
%%% // optional read params:
%%% + output(string_list) a list of output format strings,
%%% one of ['int16', 'int32', 'float32', 'coils', 'ascii', 'binary']
%%% + signed(bool_list) a list of true/false values indicating if values should be signed or not
%%%
%%% Note that, if given, all parameter functions must have the same length, this means if you have two
%%% values you want to read -> .function('coils', 'hregs') all corresponding param-functions must have the same length
%%% -> .as('val1', 'val2').output(int16, float32).from(1,2).count(2,4).signed(true, true)
%%%
%%%
%%% @end
%%% Created : 27. May 2019 09:00
%%%-------------------------------------------------------------------
-module(esp_modbus_multi).
-author("heyoka").

%% API
-behavior(df_component).

-include("faxe.hrl").
%% API
-export([init/3, process/3, options/0, handle_info/2, shutdown/1, read/3, check_options/0, metrics/0]).

-record(state, {
   ip,
   port,
   readers = [],
   num_readers = 1,
   device_address,
   function,
   starts,
   counts,
   outputs,
   interval,
   as,
   signed,
   requests,
   timer,
   align = false,
   connected = false,
   fn_id
}).

-define(FUNCTIONS, [<<"coils">>, <<"hregs">>, <<"iregs">>, <<"inputs">>, <<"memory">>]).
-define(FUNCTION_PREFIX, <<"read_">>).
-define(READ_TIMEOUT, 3000).

-spec options() -> list().
options() -> [
   {ip, string},
   {port, integer, 502},
   {every, duration, undefined},
   {align, is_set, false},
   {device, integer, 255},
   {function, binary_list},
   {from, integer_list},
   {count, integer_list},
   {as, binary_list},
   {output, string_list, undefined},
   {signed, atom_list, undefined},
   {max_connections, pos_integer, auto}].

check_options() ->
   [
      {same_length, [function, from, count, as, output, signed]}
   ].

metrics() ->
   [
      {?METRIC_BYTES_READ, meter, []}
   ].

init(NodeId, _Ins, #{} = Opts) ->
   State = init_opts(maps:to_list(Opts), #state{}),
   connection_registry:reg(NodeId, State#state.ip, State#state.port, <<"modbus_tcp">>),
   connection_registry:connecting(),
   process_flag(trap_exit, true),
   ReadRequests = build_requests(State),
   NewState = State#state{requests = ReadRequests, fn_id = NodeId},
   erlang:send_after(0, self(), connect),
   {ok, all, NewState}.

init_opts([{ip, Ip0}|Opts], State) ->
   init_opts(Opts, State#state{ip = binary_to_list(Ip0)});
init_opts([{port, Port}|Opts], State) ->
   init_opts(Opts, State#state{port = Port});
init_opts([{device, DevId}|Opts], State) ->
   init_opts(Opts, State#state{device_address = DevId});
init_opts([{every, Dur}|Opts], State) ->
   init_opts(Opts, State#state{interval = Dur});
init_opts([{function, Functions}|Opts], State) ->
   init_opts(Opts, State#state{function = [func(Fun) || Fun <- Functions]});
init_opts([{from, Starts}|Opts], State) ->
   init_opts(Opts, State#state{starts = Starts});
init_opts([{as, Aliases}|Opts], State) ->
   init_opts(Opts, State#state{as = Aliases});
init_opts([{count, Counts}|Opts], State) ->
   init_opts(Opts, State#state{counts = Counts});
init_opts([{output, Outputs}|Opts], State) ->
   init_opts(Opts, State#state{outputs = Outputs});
init_opts([{signed, Flags}|Opts], State) ->
   init_opts(Opts, State#state{signed = Flags});
init_opts([{align, Align}|Opts], State) ->
   init_opts(Opts, State#state{align = Align});
init_opts([{max_connections, MaxConn}|Opts], State) ->
   init_opts(Opts, State#state{num_readers = MaxConn});
init_opts(_O, State) ->
   State.

process(_Inport, _Item, State = #state{connected = false}) ->
   {ok, State};
process(_Inport, _Item, State = #state{connected = true}) ->
   lager:notice("read trigger"),
   handle_info(poll, State).

handle_info(connect, State = #state{}) ->
   NewState = start_connections(State),
   {ok, NewState};

handle_info(poll, State = #state{readers = Readers, requests = Requests, timer = Timer, fn_id = Id}) ->
   Ts =
      case is_record(Timer, faxe_timer) of
         true -> Timer#faxe_timer.last_time;
         false -> faxe_time:now()
      end,
   {_Time, Res} = timer:tc(?MODULE, read, [Readers, Requests, Ts]),
   case Res of
      {error, stop} ->
         lager:warning("there is a problem with reading : we stop reading"),
         {ok, State#state{timer = faxe_time:timer_cancel(Timer), connected = false}};
      {error, _Reason} ->
         lager:warning("error when reading from modbus: ~p",[_Reason]),
         {ok, State#state{timer = faxe_time:timer_next(Timer)}};
      OutPoint when is_record(OutPoint, data_point)->
         BSize = byte_size(flowdata:to_json(OutPoint)),
         node_metrics:metric(?METRIC_BYTES_READ, BSize, Id),
         node_metrics:metric(?METRIC_ITEMS_IN, 1, Id),
         {emit, {1, OutPoint}, State#state{timer = faxe_time:timer_next(Timer)}}
   end;
handle_info({'EXIT', Pid, Why}, State = #state{readers = Readers, timer = Timer}) ->
   lager:warning("Modbus Reader exited with reason: ~p",[Why]),
   connection_registry:disconnected(),
   Readers0 = lists:delete(Pid, Readers),
   _NewReader = modbus_reader:start_link(State#state.ip, State#state.port, State#state.device_address),
   NewTimer = faxe_time:timer_cancel(Timer),
   {ok, State#state{readers = Readers0, timer = NewTimer}};
handle_info({modbus, connected}, S = #state{connected = true}) ->
   {ok, S};
handle_info({modbus, Reader, connected}, S = #state{readers = Readers, num_readers = NumR}) ->
   NewReaders = [Reader|Readers],
   NewState =
   case length(NewReaders) == NumR of
      true ->
         connection_registry:connected(),
         lager:debug("ALL Modbus is connected, lets start polling ..."),
         Timer = faxe_time:init_timer(S#state.align, S#state.interval, poll),
         S#state{timer = Timer, connected = true};
      false ->
         S
   end,
   {ok, NewState#state{readers = NewReaders}};
%% if disconnected, we just wait for a connected message and stop polling in the mean time
handle_info({modbus, Reader, disconnected}, State=#state{timer = Timer, readers = Readers}) ->
   connection_registry:disconnected(),
   lager:info("Modbus reader : ~p is disconnected!!, stop polling ....", [Reader]),
   Readers0 = lists:delete(Reader, Readers),
   {ok, State#state{timer = faxe_time:timer_cancel(Timer), connected = false, readers = Readers0}};
handle_info(_E, S) ->
   lager:warning("[~p] unexpected info: ~p",[?MODULE, _E]),
   {ok, S#state{}}.

shutdown(#state{readers = Modbuss, timer = Timer}) ->
   catch (faxe_time:timer_cancel(Timer)),
   catch ([gen_server:stop(Modbus) || Modbus <- Modbuss]).

build_requests(State) ->
   Req0 = build(State),
   ReqOpts =
      case build_opts(State#state.outputs, State#state.signed) of
         [] -> [ [] || _X <- lists:seq(1, erlang:length(Req0))];
         L when is_list(L) -> L
      end,
   Requests = lists:zip(Req0, ReqOpts),
   NewRequests = lists:foldl(
      fun({{Function,Start,Amount,As}, Opts}, Acc) ->
         Acc ++ [ #{alias => As, amount => Amount, function => Function, start => Start, opts => Opts}]
      end
      , [], Requests),
   RequestsSorted = sort_by_start(NewRequests),
   find_contiguous(RequestsSorted).

%% @doc build request lists
build(#state{function = Fs, starts = Ss, counts = Cs, as = Ass}) ->
   build(Fs, Ss, Cs, Ass, []).

build([], [], [], [], Acc) ->
   Acc;
build([F|Functions], [S|Starts], [C|Counts], [As|Aliases], Acc) ->
   NewAcc = Acc ++ [{F, S, C, As}],
   build(Functions, Starts, Counts, Aliases, NewAcc).

build_opts(undefined, undefined) ->
   [];
build_opts(undefined, SignedList) when is_list(SignedList) ->
   [[{signed, Bool}] || Bool <- SignedList];
build_opts(OutFormats, undefined) when is_list(OutFormats) ->
   [[{output, binary_to_existing_atom(Out, latin1)}] || Out <- OutFormats];
build_opts(Out, Signed) when is_list(Out), is_list(Signed) ->
   F = fun
          ({<<>>, S}) ->  [{signed, S}];
          ({O, <<>>}) -> [{output, binary_to_existing_atom(O, latin1)}];
          ({O, S}) -> [{output, binary_to_existing_atom(O, latin1)}, {signed, S}]
       end,
   lists:map(F, lists:zip(Out, Signed)).

sort_by_start(List) ->
   lists:sort(
      fun(#{start := StartA}, #{start := StartB}) ->
         StartA < StartB end,
      List).

find_contiguous(L) ->
   F = fun(
       #{function := Function, start := Start, amount := Amount, alias := As, opts := Opts} = E,
       {LastStart, Current = #{aliases := CAs, count := CCount, function := CFunc, opts := COpts}, Partitions}) ->
      case (Function == CFunc) andalso (LastStart + Amount == Start) andalso (Opts == COpts) of
         true ->
            NewCurrent = Current#{count => CCount+1, aliases => CAs++[As], amount => (CCount+1)*Amount},
            {Start, NewCurrent, Partitions};
         false ->
            {Start,
               E#{aliases => [As], count => 1, amount => Amount}, Partitions++[Current]
            }
      end
       end,
   {_Last, Current, [_|Parts]} =
      lists:foldl(F, {-1, #{aliases => [], amount => -1, function => nil, count => 0, opts => nil}, []}, L),
   All = [Current|Parts],
   All.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% read all prepared requests
read(Readers, Reqs, Ts) ->
   read_all_multi(Readers, #data_point{ts = Ts}, Reqs).

%% @doc distribute the read requests to all available reader processes
%% if there are more requests than readers, some reader processes will get more than one request
read_all_multi(Clients, Point, Reqs) ->
   ReadRef = make_ref(),
   {Waiting, _} =
   lists:foldl(
      fun(Req, {Waits, [Client|ClientList]}) ->
         Client ! {read, ReadRef, Req},
         {[Client|Waits], ClientList ++ [Client]}
      end,
      {[], Clients},
      Reqs
   ),
   collect(Waiting, ReadRef, Point).

collect([], _Ref, Point) -> Point;
collect(Waiting, ReadRef, Point) ->
   receive
      {modbus_data, _Client, ReadRef, {error, stop}} ->
         {error, stop};
      {modbus_data, _Client, ReadRef, {error, _Reason}} ->
         collect(Waiting, ReadRef, Point);
      {modbus_data, Client, ReadRef, {ok, Values}} ->
         collect(lists:delete(Client, Waiting), ReadRef, flowdata:set_fields(Point, Values));
      %% flush possibly old values from previous read requests, we do not want these anymore
      {modbus_data, _Client, OldRef, _WhatEver} when OldRef /= ReadRef ->
         collect(Waiting, ReadRef, Point)

   after ?READ_TIMEOUT -> {error, timeout}
   end.

func(BinString) ->
   %% make sure atoms are known at this point
   code:ensure_loaded(modbus),
   F =
      case lists:member(BinString, ?FUNCTIONS) of
         true -> binary_to_existing_atom(<< ?FUNCTION_PREFIX/binary, BinString/binary >>, utf8);
         false -> erlang:error("Modbus function " ++ binary_to_list(BinString) ++ " not available")
      end,
   F.

start_connections(State = #state{ip = Ip, port = Port, device_address = Dev, requests = Req, num_readers = Num}) ->
   ConnNum =
      case Num of
         _ when is_integer(Num) andalso length(Req) > Num -> Num;
         _ -> length(Req)
      end,
   lists:map(
      fun(_E) ->
         {ok, Reader} = modbus_reader:start_link(Ip, Port, Dev),
         Reader
      end,
   lists:seq(1, ConnNum)
   ),
   lager:info("started ~p reader connections",[ConnNum]),
   State#state{num_readers = ConnNum}.


-ifdef(TEST).

build_find_contiguous_test() ->
   Requests = [
      #{alias => <<"ActiveEnergyRcvd">>,
         amount => 2,
         function => read_hregs,
         opts => [{output,float32}],start => 2701},
      #{alias => <<"ActiveEnergyDelvd">>,
         amount => 2,
         function => read_hregs,
         opts => [{output,float32}],
         start => 2699},
      #{alias => <<"ReactiveEnergyRcvd">>,
         amount => 2,
         function => read_hregs,
         opts => [{output,float32}],
         start => 2709},
      #{alias => <<"ReactiveEnergyDelvd">>,
         amount => 2,
         function => read_hregs,
         opts => [{output,float32}],
         start => 2707},
      #{alias => <<"ApparentEnergyRcvd">>,
         amount => 2,
         function => read_hregs,
         opts => [{output,float32}],
         start => 2717},
      #{alias => <<"ApparentEnergyDelvd">>,
         amount => 2,
         function => read_hregs,
         opts => [{output,float32}],
         start => 2715},
      #{alias => <<"MaximalCurrentValue">>,
         amount => 2,
         function => read_hregs,
         opts => [{output,float32}],
         start => 3009}
   ],
   Expected = [
      #{alias => <<"MaximalCurrentValue">>,
         aliases => [<<"MaximalCurrentValue">>],
         amount => 2,
         count => 1,
         function => read_hregs,
         opts => [{output,float32}],
         start => 3009},
      #{alias => <<"ActiveEnergyDelvd">>,
         aliases => [<<"ActiveEnergyDelvd">>,<<"ActiveEnergyRcvd">>],
         amount => 4,
         count => 2,
         function => read_hregs,
         opts => [{output,float32}],
         start => 2699},
      #{alias => <<"ReactiveEnergyDelvd">>,
         aliases => [<<"ReactiveEnergyDelvd">>,<<"ReactiveEnergyRcvd">>],
         amount => 4,
         count => 2,
         function => read_hregs,
         opts => [{output,float32}],
         start => 2707},
      #{alias => <<"ApparentEnergyDelvd">>,
         aliases => [<<"ApparentEnergyDelvd">>,<<"ApparentEnergyRcvd">>],
         amount => 4,
         count => 2,
         function => read_hregs,
         opts => [{output,float32}],
         start => 2715}
   ],
   ?assertEqual(Expected, find_contiguous(sort_by_start(Requests))).


-endif.
