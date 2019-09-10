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
%%% + every(duration) interval for reading values from a modbus device
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
%%% Note that, if given, all parameter function must have the same length, this mean if you have two
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
-export([init/3, process/3, options/0, handle_info/2, shutdown/1, read/2, check_options/0]).

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
   timer_ref,
   requests,
   last_poll_time
}).

-define(FUNCTIONS, [<<"coils">>, <<"hregs">>, <<"iregs">>, <<"inputs">>, <<"memory">>]).
-define(FUNCTION_PREFIX, <<"read_">>).

-spec options() -> list().
options() -> [
   {ip, string},
   {port, integer, 502},
   {every, duration, "1s"},
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
%%      {not_empty, [every]}
   ].

init(_NodeId, _Ins, #{} = Opts) ->
   lager:notice("++++ ~p ~ngot opts: ~p ~n",[_NodeId, maps:to_list(Opts)]),

   State = init_opts(maps:to_list(Opts), #state{}),
   lager:notice("MODBUS State is: ~p", [State]),
   {ok, Modbus} = modbus:connect(
      State#state.ip, State#state.port, State#state.device_address),
   erlang:monitor(process, Modbus),
   Req0 = build(State),
   lager:notice("BUILT MODBUS Options: ~p", [Req0]),
   ReqOpts =
      case build_opts(State#state.outputs, State#state.signed) of
         [] -> [ [] || X <- lists:seq(1, erlang:length(Req0))];
         L when is_list(L) -> L
      end,

   Requests = lists:zip(Req0, ReqOpts),
   lager:notice("MODBUS Requests: ~p",[Requests]),
   NewState = State#state{client = Modbus, requests = Requests},
   lager:info("~p init state is: ~p",[?MODULE, NewState#state.requests]),
   {ok, all, NewState}.

init_opts([{ip, Ip0}|Opts], State) ->
   init_opts(Opts, State#state{ip = binary_to_list(Ip0)});
init_opts([{port, Port}|Opts], State) ->
   init_opts(Opts, State#state{port = Port});
init_opts([{device, DevId}|Opts], State) ->
   init_opts(Opts, State#state{device_address = DevId});
init_opts([{every, Dur}|Opts], State) ->
   init_opts(Opts, State#state{interval = faxe_time:duration_to_ms(Dur)});
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
init_opts(_O, State) ->
   lager:notice("done init_opts when :~p",[_O]),
   State.

process(_In, #data_batch{points = _Points} = _Batch, State = #state{}) ->
   {ok, State};
process(_Inport, #data_point{} = _Point, State = #state{}) ->
   {ok, State}.

handle_info(poll, State=#state{client = Modbus, requests = Requests, interval = Interval}) ->
   {Time, Res} = timer:tc(?MODULE, read, [Modbus, Requests]),
   NewState =
      case Res of
         {error, stop} ->
            State#state{timer_ref = undefined};
         {ok, OutPoint} ->
            dataflow:emit(OutPoint),
            lager:info("emit after: ~p: ~p" ,[Time, OutPoint]),
            poll(State)
      end,
   {ok, NewState};
handle_info({'DOWN', _MonitorRef, process, _Object, Info}, State=#state{ip = Ip, port = Port, device_address = Device}) ->
   lager:warning("Modbus process is DOWN with : ~p !", [Info]),
   {ok, Modbus} = modbus:connect([{host, Ip}, {port,Port}, {unit_id, Device}, {max_retries, 7}]),
   erlang:monitor(process, Modbus),
   {ok, State#state{client = Modbus}};
handle_info({modbus, _From, connected}, S) ->
   lager:notice("Modbus is connected, lets start polling ..."),
   NewState = poll_now(S),
   {ok, NewState};
%% if disconnected, we just wait for a connected message and stop polling in the mean time
handle_info({modbus, _From, disconnected}, State=#state{timer_ref = TRef}) ->
   lager:notice("Modbus is disconnected!!, stop polling ...."),
   cancel_timer(TRef),
   {ok, State#state{timer_ref = undefined}};
handle_info(E, S) ->
   lager:warning("unexpected: ~p~n", [E]),
   {ok, S#state{}}.

shutdown(#state{client = Modbus, timer_ref = Timer}) ->
   catch (cancel_timer(Timer)),
   catch (modbus:disconnect(Modbus)).

poll_now(State=#state{}) ->
   TRef = erlang:send_after(0, self(), poll),
   State#state{timer_ref = TRef, last_poll_time = faxe_time:now()}.

poll(State=#state{last_poll_time = LPT, interval = Interval}) ->
   At = LPT + Interval,
   NewTRef = faxe_time:send_at(At, poll),
   State#state{timer_ref = NewTRef, last_poll_time = At}.

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
read(Client, Reqs) ->
   read(Client, #data_point{ts = faxe_time:now()}, Reqs).

read(_Client, Point, []) ->
   {ok, Point};
read(Client, Point, [{{Fun, Start, Count, As}, Opts} | Reqs]) ->
   lager:notice("read modbus:~p(~p)",[Fun,[Client, Start, Count, Opts]]),
   Res = modbus:Fun(Client, Start, Count, Opts),
   lager:notice("modbus result: ~p", [Res]),
   case Res of
      {error, disconnected} ->
         {error, stop};
      {error, _Reason} ->
         read(Client, Point, Reqs);
      Data ->
         FData =
         case Data of
            [D] -> D;
            _ -> Data
         end,
         %% we assume, if an output option is given, then a single value (instead a list) is desired
%%         FData =
%%         case proplists:get_value(output, Opts) of
%%            undefined -> Data;
%%            _ ->  case Data of [] -> []; [D] -> D end
%%         end,
         NewPoint = flowdata:set_field(Point, As, FData),
         read(Client, NewPoint, Reqs)
   end.


cancel_timer(TRef) when is_reference(TRef) -> erlang:cancel_timer(TRef);
cancel_timer(_) -> ok.

func(BinString) ->
   %% make sure atoms are known at this point
   code:ensure_loaded(modbus),
   F =
      case lists:member(BinString, ?FUNCTIONS) of
         true -> binary_to_existing_atom(<< ?FUNCTION_PREFIX/binary, BinString/binary >>, utf8);
         false -> erlang:error("Modbus function " ++ binary_to_list(BinString) ++ " not available")
      end,
   lager:notice("Function for Modbus Node is: ~p",[F]),
   F.

