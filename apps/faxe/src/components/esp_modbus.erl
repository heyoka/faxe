%%%-------------------------------------------------------------------
%%% @author heyoka
%%% @copyright (C) 2019, <COMPANY>
%%% @doc
%%% pull data via modbus tcp, supported read functions are :
%%% [<<"coils">>, <<"hregs">>, <<"iregs">>, <<"inputs">>, <<"memory">>]
%%% @end
%%% Created : 27. May 2019 09:00
%%%-------------------------------------------------------------------
-module(esp_modbus).
-author("heyoka").

%% API
-behavior(df_component).

-include("faxe.hrl").
%% API
-export([init/3, process/3, options/0, handle_info/2, shutdown/1]).

-record(state, {
  ip,
  port,
  client,
  device_address,
  function,
  start,
  count,
  interval,
  as,
  timer_ref
}).

-define(FUNCTIONS, [<<"coils">>, <<"hregs">>, <<"iregs">>, <<"inputs">>, <<"memory">>]).
-define(FUNCTION_PREFIX, <<"read_">>).

options() -> [{ip, string}, {port, integer, 502},
  {every, binary, "1s"}, {device, integer, 255},
  {function, binary, <<"coils">>}, {from, integer}, {count, integer, 1}, {as, binary_list}].

init(_NodeId, _Ins,
    #{ip := Ip0, port := Port, device := DevAddress, every := Dur,
      function := Func, from := Start, count := Count, as := As}=Opts) ->
  lager:notice("++++ ~p ~ngot opts: ~p ~n",[_NodeId, Opts]),
  Ip = binary_to_list(Ip0),
  {ok, Modbus} = modbus:connect(Ip, Port, DevAddress),
  erlang:monitor(process, Modbus),

  Interval = faxe_time:duration_to_ms(Dur),
  {ok, all,
    #state{ip = Ip, port = Port, device_address = DevAddress, start = Start, count = Count, as = As,
      client = Modbus, function = func(Func), interval = Interval}}.

process(_In, #data_batch{points = _Points} = _Batch, State = #state{}) ->
  {ok, State};
process(_Inport, #data_point{} = _Point, State = #state{}) ->
  {ok, State}.

handle_info(poll, State=#state{client = Modbus, start = Start, count = Count,
    function = Func, interval = Interval, as = Aliases}) ->
  Res = modbus:Func(Modbus, Start, Count),
  NewState =
  case Res of
    {error, disconnected} ->
      State#state{timer_ref = undefined};
    {error, Reason} ->
      lager:warning("Modbus data reading failed with : ~p",[Reason]),
      State#state{timer_ref = poll(Interval)};
    Data ->
      OutPoint = build_point(Data, Aliases),
%%      lager:info("Result from modbus function ~p : ~p",[Func, Res]),
      %% emit the result
      dataflow:emit(OutPoint),
      State#state{timer_ref = poll(Interval)}
  end,
  {ok, NewState};
handle_info({'DOWN', _MonitorRef, process, _Object, Info}, State=#state{ip = Ip, port = Port, device_address = Device}) ->
  lager:warning("Modbus process is DOWN with : ~p !", [Info]),
  {ok, Modbus} = modbus:connect(Ip, Port, Device),
  erlang:monitor(process, Modbus),
  {ok, State#state{client = Modbus}};
handle_info({modbus, _From, connected}, S) ->
  lager:notice("Modbus is connected, lets start polling ..."),
  TRef = poll(0),
  {ok, S#state{timer_ref = TRef}};
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

poll(Interval) ->
  erlang:send_after(Interval, self(), poll).

cancel_timer(TRef) when is_reference(TRef) -> erlang:cancel_timer(TRef);
cancel_timer(_) -> ok.

func(BinString) ->
  F =
  case lists:member(BinString, ?FUNCTIONS) of
    true -> binary_to_existing_atom(<< ?FUNCTION_PREFIX/binary, BinString/binary >>, utf8);
    false -> erlang:error("Modbus function " ++ binary_to_list(BinString) ++ " not available")
  end,
  lager:notice("Function for Modbus Node is: ~p",[F]),
  F.

build_point(ResultList, AliasList) when is_list(ResultList), is_list(AliasList) ->
  build(#data_point{ts=faxe_time:now()}, ResultList, AliasList).

build(Point=#data_point{}, [],[]) ->
  Point;
build(Point=#data_point{}, [Res|R],[Alias|A]) ->
  NewPoint = flowdata:set_field(Point, Alias, Res),
  build(NewPoint, R, A).


