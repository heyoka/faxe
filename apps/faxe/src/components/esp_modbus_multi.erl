%%%-------------------------------------------------------------------
%%% @author heyoka
%%% @copyright (C) 2019, <COMPANY>
%%% @doc
%%% pull data via modbus tcp, supported read functions are :
%%% [<<"coils">>, <<"hregs">>, <<"iregs">>, <<"inputs">>, <<"memory">>]
%%% read multiple values with possibly different functions at once
%%% @end
%%% Created : 27. May 2019 09:00
%%%-------------------------------------------------------------------
-module(esp_modbus_multi).
-author("heyoka").

%% API
-behavior(df_component).

-include("faxe.hrl").
%% API
-export([init/3, process/3, options/0, handle_info/2, shutdown/1, read/2]).

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
options() -> [{ip, string}, {port, integer, 502},
   {every, binary, "1s"}, {device, integer, 255},
   {function, binary_list},
   {from, integer_list},
   {count, integer_list},
   {as, binary_list},
   {output, string_list},
   {signed, atom_list}].

init(_NodeId, _Ins, #{} = Opts) ->
   lager:notice("++++ ~p ~ngot opts: ~p ~n",[_NodeId, Opts]),

   State = init_opts(maps:to_list(Opts), #state{}),
   {ok, Modbus} = modbus:connect(
      State#state.ip, State#state.port, State#state.device_address),
   erlang:monitor(process, Modbus),

   NewState = State#state{client = Modbus, requests = build(State)},
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
build(#state{function = Fs, starts = Ss, counts = Cs, outputs = Os, signed = Sigs, as = Ass}) ->
   build(Fs, Ss, Cs, Os, Sigs, Ass, []).

build([], [], [], [], [], [], Acc) ->
   Acc;
build([F|Functions], [S|Starts], [C|Counts], [Out|Outputs], [Signed|Flags], [As|Aliases], Acc) ->
   NewAcc = Acc ++ [{F, S, C, build_opts(Out, Signed), As}],
   build(Functions, Starts, Counts, Outputs, Flags, Aliases, NewAcc).

build_opts(<<>>, Bool) when is_atom(Bool) ->
   [{signed, Bool}];
build_opts(Out, Signed) when is_binary(Out), is_atom(Signed) ->
   [{output, binary_to_existing_atom(Out, latin1)}, {signed, Signed}].

%% read all prepared requests
read(Client, Reqs) ->
   read(Client, #data_point{ts = faxe_time:now()}, Reqs).

read(_Client, Point, []) ->
   {ok, Point};
read(Client, Point, [{Fun, Start, Count, Opts, As} | Reqs]) ->
   lager:notice("read modbus:~p(~p)",[Fun,[Client, Start, Count, Opts]]),
   Res = modbus:Fun(Client, Start, Count, Opts),
   case Res of
      {error, disconnected} ->
         {error, stop};
      {error, _Reason} ->
         read(Client, Point, Reqs);
      Data ->
         NewPoint = flowdata:set_field(Point, As, Data),
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

