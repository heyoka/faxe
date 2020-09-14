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
-module(esp_modbus).
-author("heyoka").

%% API
-behavior(df_component).

-include("faxe.hrl").
%% API
-export([init/3, process/3, options/0, handle_info/2, shutdown/1, read/3, check_options/0, metrics/0]).

-record(state, {
   ip,
   port,
   client,
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
   {signed, atom_list, undefined}].

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
   {ok, Modbus} = modbus:connect(
      State#state.ip, State#state.port, State#state.device_address),
   erlang:monitor(process, Modbus),
   Req0 = build(State),
   ReqOpts =
      case build_opts(State#state.outputs, State#state.signed) of
         [] -> [ [] || _X <- lists:seq(1, erlang:length(Req0))];
         L when is_list(L) -> L
      end,
   Requests = lists:zip(Req0, ReqOpts),
   NewState = State#state{client = Modbus, requests = Requests, fn_id = NodeId},
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
init_opts(_O, State) ->
   State.

process(_Inport, _Item, State = #state{connected = false}) ->
   {ok, State};
process(_Inport, _Item, State = #state{connected = true}) ->
   handle_info(poll, State).

handle_info(poll, State = #state{client = Modbus, requests = Requests, timer = Timer, fn_id = Id}) ->
   Ts = Timer#faxe_timer.last_time,
   {_Time, Res} = timer:tc(?MODULE, read, [Modbus, Requests, Ts]),
   case Res of
      {error, stop} ->
         {ok, State#state{timer = faxe_time:timer_cancel(Timer)}};
      {error, _Reason} ->
         {ok, State#state{timer = faxe_time:timer_next(Timer)}};
      {ok, OutPoint} ->
         BSize = byte_size(flowdata:to_json(OutPoint)),
         node_metrics:metric(?METRIC_BYTES_READ, BSize, Id),
         node_metrics:metric(?METRIC_ITEMS_IN, 1, Id),
         {emit, {1, OutPoint}, State#state{timer = faxe_time:timer_next(Timer)}}
   end;
handle_info({'DOWN', _MonitorRef, process, _Object, Info},
    State=#state{ip = Ip, port = Port, device_address = Device, timer = Timer}) ->
   connection_registry:disconnected(),
   NewTimer = faxe_time:timer_cancel(Timer),
   lager:warning("Modbus process is DOWN with : ~p !", [Info]),
   {ok, Modbus} = modbus:connect([{host, Ip}, {port,Port}, {unit_id, Device}, {max_retries, 7}]),
   erlang:monitor(process, Modbus),
   {ok, State#state{client = Modbus, timer = NewTimer}};
handle_info({modbus, _From, connected}, S = #state{}) ->
   connection_registry:connected(),
%%   lager:notice("Modbus is connected, lets start polling ..."),
   Timer = faxe_time:init_timer(S#state.align, S#state.interval, poll),
   NewState = S#state{timer = Timer, connected = true},
   {ok, NewState};
%% if disconnected, we just wait for a connected message and stop polling in the mean time
handle_info({modbus, _From, disconnected}, State=#state{timer = Timer}) ->
   connection_registry:disconnected(),
   lager:notice("Modbus is disconnected!!, stop polling ...."),
   {ok, State#state{timer = faxe_time:timer_cancel(Timer), connected = false}};
handle_info(_E, S) ->
   {ok, S#state{}}.

shutdown(#state{client = Modbus, timer = Timer}) ->
   catch (faxe_time:timer_cancel(Timer)),
   catch (modbus:disconnect(Modbus)).

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


%% read all prepared requests
read(Client, Reqs, Ts) ->
   do_read(Client, #data_point{ts = Ts}, Reqs).

do_read(_Client, Point, []) ->
   {ok, Point};
do_read(Client, Point, [{{Fun, Start, Count, As}=F, Opts} | Reqs]) ->
   Res = modbus:Fun(Client, Start, Count, Opts),
   case Res of
      {error, disconnected} ->
         {error, stop};
      {error, _Reason} ->
         lager:error("error reading from modbus: ~p (~p)",[_Reason, F]),
         read(Client, Point, Reqs);
      Data ->
         FData =
         case Data of
            [D] -> D;
            _ -> Data
         end,
         NewPoint = flowdata:set_field(Point, As, FData),
         read(Client, NewPoint, Reqs)
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

