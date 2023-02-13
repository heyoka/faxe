%%%-------------------------------------------------------------------
%%% @author heyoka
%%% @copyright (C) 2022
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
%%% Created : 3. May 2022
%%%-------------------------------------------------------------------
-module(esp_modbus_read).
-author("heyoka").

%% API
-behavior(df_component).

-include("faxe.hrl").
%% API
-export([
   init/3,
   process/3,
   options/0,
   handle_info/2,
   shutdown/1,
   read/4,
   check_options/0,
   metrics/0,
   emits/0]).

-record(state, {
   ip,
   port,
   client,
   read_timeout,
   device_address,
   function,
   starts,
   counts,
   outputs,
   interval,
   as,
   signed,
   round,
   requests,
   timer,
   align = false,
   connected = false,
   fn_id,
   byte_size_total = 0
}).

-define(FUNCTIONS, [<<"coils">>, <<"hregs">>, <<"iregs">>, <<"inputs">>, <<"memory">>]).
-define(OUT_TYPES, [<<"int16">>, <<"int32">>, <<"float32">>, <<"double">>, <<"coils">>,<<"ascii">>, <<"binary">>]).
-define(FUNCTION_PREFIX, <<"read_">>).

-spec options() -> list().
options() -> [
   {ip, string},
   {port, integer, 502},
   {every, duration, undefined},
   {align, boolean, true},
   {device, integer, 255},
   {function, string_list},
   {from, integer_list},
   {count, integer_list},
   {as, binary_list},
   {output, string_list, undefined},
   {signed, atom_list, undefined},
   {round, integer, undefined},
   {timeout, duration, <<"5s">>}
].

check_options() ->
   [
      {same_length, [function, from, count, as, output, signed]},
      {one_of, function, ?FUNCTIONS},
      {one_of, output, ?OUT_TYPES}
   ].

metrics() ->
   [
      {?METRIC_BYTES_READ, meter, []}
   ].

emits() -> point.

init(NodeId, _Ins, #{timeout := Timeout} = Opts) ->
   State = init_opts(maps:to_list(Opts), #state{}),
   connection_registry:reg(NodeId, State#state.ip, State#state.port, <<"modbus_tcp">>),
   connection_registry:connecting(),
   ReadRequests = build_requests(State),
   Bytes = lists:foldl(fun(#{amount := ByteSize}, Acc) -> Acc + ByteSize end, 0, ReadRequests),
   ReadTimeout = faxe_time:duration_to_ms(Timeout),
   NewState =
      State#state{requests = ReadRequests, fn_id = NodeId, read_timeout = ReadTimeout, byte_size_total = Bytes},
   erlang:send_after(0, self(), connect),
   %% monitor time_offsets
   erlang:monitor(time_offset, clock_service),
   {ok, all, NewState}.

init_opts([{'_name', _DisplayName}|Opts], State) ->
   init_opts(Opts, State);
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
init_opts([{round, Prec}|Opts], State) ->
   init_opts(Opts, State#state{round = Prec});
init_opts(_O, State) ->
   State.

process(_Inport, _Item, State = #state{connected = false}) ->
   {ok, State};
process(_Inport, _Item, State = #state{connected = true}) ->
%%   lager:notice("read trigger"),
   handle_info(poll, State).

handle_info(connect, State = #state{}) ->
   NewState = start_connection(State),
   {ok, NewState};

handle_info(poll, State = #state{client = Client, requests = Requests,
      timer = Timer, fn_id = Id, read_timeout = Timeout}) ->
   Ts =
      case is_record(Timer, faxe_timer) of
         true -> Timer#faxe_timer.last_time;
         false -> faxe_time:now()
      end,
   Res = read(Client, Requests, #data_point{ts = Ts}, Timeout),
   case Res of
      {error, _Reason} ->
         lager:warning("error when reading from modbus, request cancelled: ~p",[_Reason]),
         {ok, State#state{timer = faxe_time:timer_next(Timer)}};
      OutPoint when is_record(OutPoint, data_point) ->
         node_metrics:metric(?METRIC_BYTES_READ, State#state.byte_size_total, Id),
         node_metrics:metric(?METRIC_ITEMS_IN, 1, Id),
         NewOutPoint = maybe_round(OutPoint, State),
         {emit, {1, NewOutPoint}, State#state{timer = faxe_time:timer_next(Timer)}}
   end;
handle_info({'DOWN', _MonitorRef, process, Object, _Info}, State=#state{client = Object, timer = Timer}) ->
   lager:notice("modbus client is DOWN"),
   erlang:send_after(200, self(), connect),
   connection_registry:disconnected(),
   {ok, State#state{client = undefined, connected = false, timer = faxe_time:timer_cancel(Timer)}};

handle_info({modbus, Client, connected}, S = #state{client = Client}) ->
   connection_registry:connected(),
   Timer = faxe_time:init_timer(S#state.align, S#state.interval, poll),
   {ok, S#state{timer = Timer, connected = true}};
%% if disconnected, we just wait for a connected message and stop polling in the mean time
handle_info({modbus, Client, disconnected}, State=#state{timer = Timer, client = Client}) ->
   connection_registry:disconnected(),
   lager:notice("Modbus Client is disconnected!!, stop polling ....", []),
   {ok, State#state{timer = faxe_time:timer_cancel(Timer), connected = false}};
handle_info({'CHANGE', _MonitorReference, time_offset, clock_service, _NewTimeOffset}, State=#state{timer = Timer}) ->
   faxe_time:timer_cancel(Timer),
   lager:warning("<~p> TIME_OFFSET changed ", [{?MODULE, self()}]),
   Timer = faxe_time:init_timer(State#state.align, State#state.interval, poll),
   NewState = State#state{timer = Timer},
   {ok, NewState};

handle_info(_E, S) ->
   {ok, S#state{}}.

maybe_round(Point, #state{round = undefined}) ->
   Point;
maybe_round(Point, #state{round = Precision, as = Aliases, outputs = OutTypes}) ->
   AsTypes = lists:zip(Aliases, OutTypes),
   RoundFn =
   fun
      ({As, DType}, Point0) when DType =:= <<"float32">> orelse DType =:= <<"double">> ->
         flowdata:set_field(Point0, As, faxe_util:round_float(flowdata:field(Point0, As), Precision));
      (_, Point1) -> Point1
   end,
   lists:foldl(RoundFn, Point, AsTypes).


shutdown(#state{client = Client, timer = Timer}) ->
   connection_registry:disconnected(),
   catch (faxe_time:timer_cancel(Timer)),
   catch (modbus:disconnect(Client)).

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

-spec read(pid(), list(map()), #data_point{}, non_neg_integer()) -> #data_point{}|{error, Reason :: term()}.
read(_Client, [], Point, _T) ->
   Point;
read(_Client, _Reqs, {error, _R} = Err, _Timeout) ->
   Err;
read(Client, [Request|Reqs], Point, Timeout) ->
   NewPoint =
   case do_read(Request, Client, Timeout) of
      {error, _What} = Error ->
         Error;
      Res when is_list(Res) -> flowdata:set_fields(Point, Res)
   end,
   read(Client, Reqs, NewPoint, Timeout).


do_read(
    #{function := Fun, start := Start, amount := Amount, opts := Opts, aliases := As}, Client, Timeout) ->
   Res = modbus:Fun(Client, Start, Amount, Opts),
   case Res of
      {error, disconnected} ->
         {error, disconnected};
      {error, _Reason} ->
         {error, _Reason};
      {ok, TId} ->
         case recv(TId, Timeout) of
            {ok, Data} -> lists:zip(As, Data);
            Other -> Other
         end
   end.


recv(TId, Timeout) ->
   receive
      {modbusdata, {ok, TId, Data}} ->
         {ok, Data};
      {modbusdata, {ok, NotOurTId, _Data}} ->
         {error, {wrong_tid_returned, NotOurTId}};
      {modbusdata, {error, TId, What}} ->
         {error, What}

   after Timeout -> {error, timeout}
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

start_connection(State = #state{ip = Ip, port = Port, device_address = Dev}) ->
   {ok, Modbus} = modbus:connect(Ip, Port, Dev),
   erlang:monitor(process, Modbus),
   State#state{client = Modbus, connected = false}.


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

round_values_test() ->
   P = test_point(),
   State = test_state(),
   Expected = #data_point{fields = #{<<"val1">> => 3, <<"val2">> => 2.456549, <<"val3">> => 134.554}},
   ?assertEqual(Expected, maybe_round(P, State)).
no_rounding_test() ->
   P = test_point(),
   State0 = test_state(),
   State = State0#state{round = undefined},
   ?assertEqual(P, maybe_round(P, State)).

test_point() ->
   #data_point{fields = #{<<"val1">> => 3, <<"val2">> => 2.45654866543, <<"val3">> => 134.554}}.
test_state() ->
   #state{round = 6,
      as = [<<"val1">>, <<"val2">>, <<"val3">>],
      outputs = [<<"int16">>, <<"double">>, <<"float32">>]}.


-endif.
