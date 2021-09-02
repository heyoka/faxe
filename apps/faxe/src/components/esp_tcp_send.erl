%%%-------------------------------------------------------------------
%%% @author heyoka
%%% @copyright (C) 2020
%%% @doc
%%%
%%% @end
%%% Created : 29. August 2020 09:00
%%%-------------------------------------------------------------------
-module(esp_tcp_send).
-author("heyoka").

%% API
-behavior(df_component).

-include("faxe.hrl").
%% API
-export([init/3, process/3, options/0, handle_info/2, shutdown/1, check_options/0, metrics/0]).

-record(state, {
  ip,
  port,
  socket,
  socket_active = once,
  packet,
  response_as,
  response_timeout,
  response_is_json,
  response_tref,
  waiting_response = false,
  connector,
  connected,
  interval,
  count,
  prefix,
  timer_ref,
  msg_text,
  msg_json,
  last_item,
  fn_id
}).

-define(SOCKOPTS, [{active, once}, binary]).

options() -> [
  {ip, binary},
  {port, integer},
  {packet, integer, 2},
  {every, duration, undefined},
  {response_as, string, undefined},
  {response_json, is_set, false},
  {response_timeout, duration, <<"5s">>},
  {msg_text, string, undefined},
  {msg_json, string, undefined}].

check_options() ->
  [
%%    {one_of_params, [msg_text, msg_json]}
  ]
.

metrics() ->
  [
    {?METRIC_BYTES_SENT, meter, []}
  ].

init(NodeId, _Ins,
    #{ip := Ip, port := Port, every := Dur, msg_text := Text, msg_json := Json,
      response_as := RespAs, response_timeout := RTimeout, response_json := RespJson, packet := Packet}) ->

  connection_registry:reg(NodeId, Ip, Port, <<"tcp">>),

  Interval =
    case Dur of
      undefined -> undefined;
      _ -> faxe_time:duration_to_ms(Dur)
    end,
  ResponseTimeout = faxe_time:duration_to_ms(RTimeout),
  Reconnector = faxe_backoff:new({200, 4200}),
  {ok, Recon} = faxe_backoff:execute(Reconnector, reconnect),

  {ok, all,
    #state{
      ip = Ip,
      port = Port,
      packet = Packet,
      response_as = RespAs,
      response_timeout = ResponseTimeout,
      response_is_json = RespJson,
      connector = Recon,
      interval = Interval,
      msg_text = Text,
      msg_json = Json,
      last_item = flowdata:new(),
      fn_id = NodeId
    }
  }.

process(_In, Item, State = #state{connected = false}) ->
  {ok, State#state{last_item = Item}};
process(_In, Item, State = #state{}) ->
  NewState = State#state{last_item = Item},
  {ok, send(NewState)}.

%% response
handle_info(reconnect, State = #state{}) ->
  NewState = connect(State),
  {ok, sender(NewState)};
handle_info({tcp, Socket, _Data0}, State=#state{response_as = undefined}) ->
%%  lager:notice("got tcp data: ~p, but response will be ignored",[_Data0]),
  socket_active(Socket),
  {ok, State};
handle_info({tcp, Socket, _Data0}, State=#state{waiting_response = false}) ->
%%  lager:notice("got tcp data: ~p, but response timeout exceeded",[_Data0]),
  socket_active(Socket),
  {ok, State};
handle_info({tcp, Socket, Data0}, State=#state{response_as = As, response_is_json = Json}) ->
  Data = case Json of true -> jiffy:decode(Data0, [return_maps]); false -> Data0 end,
%%  lager:notice("Data got from TCP:  ~p",[Data]),
  State0 = cancel_response_timeout(State),
  P = flowdata:set_field(#data_point{ts = faxe_time:now()}, As, Data),
  socket_active(Socket),
%%  NewState = sender(State0),
  {emit, {1, P}, State0};
handle_info(send, State=#state{}) ->
  {ok, send(State)};
handle_info({tcp_closed, _S}, S=#state{connector = Reconnector, timer_ref = TRef}) ->
  connection_registry:disconnected(),
  catch (erlang:cancel_timer(TRef)),
  State0 = cancel_response_timeout(S),
  {ok, Recon} = faxe_backoff:execute(Reconnector, reconnect),
  NewState = State0#state{connector = Recon, connected = false},
  {ok, NewState};
handle_info({tcp_error, Socket, _}, S=#state{timer_ref = Timer}) ->
  catch (erlang:cancel_timer(Timer)),
  NewState = sender(S),
  socket_active(Socket),
  {ok, NewState};
handle_info(response_timeout, State) ->
%%  lager:info("response timeout!"),
  {ok, State#state{waiting_response = false}};
handle_info(E, S) ->
  lager:warning("unexpected: ~p~n", [E]),
  {ok, S}.

shutdown(#state{socket = Sock, timer_ref = Timer, response_tref = RTimer}) ->
  catch (erlang:cancel_timer(Timer)),
  catch (erlang:cancel_timer(RTimer)),
  catch (gen_tcp:close(Sock)).

send(#state{msg_json = undefined, msg_text = undefined, last_item = LastItem} = State) ->
%%  lager:notice("send: ~p",[LastItem]),
  Msg = flowdata:to_json(LastItem),
  do_send(Msg, maybe_start_timeout(State));
send(#state{msg_json = undefined, msg_text = Text} = State) ->
%%  lager:notice("send: ~p",[Text]),
  do_send(Text, maybe_start_timeout(State));
send(#state{msg_json = Json} = State) ->
  do_send(jiffy:encode(Json), maybe_start_timeout(State)).

do_send(Msg, State = #state{socket = Socket, fn_id = FNId}) ->
  case gen_tcp:send(Socket, Msg) of
    ok ->
      node_metrics:metric(?METRIC_ITEMS_OUT, 1, FNId),
      node_metrics:metric(?METRIC_BYTES_SENT, faxe_util:bytes(Msg), FNId),
      sender(State);
    {error, What} ->
      lager:warning("Error sending tcp data: ~p",[What]),
      catch (gen_tcp:close(Socket)),
      connect(State)
  end.

sender(State = #state{connected = false}) ->
  State;
sender(State = #state{interval = undefined}) ->
  State;
sender(State = #state{interval = Interval}) ->
  TRef = erlang:send_after(Interval, self(), send),
  State#state{timer_ref = TRef}.

maybe_start_timeout(State = #state{response_as = undefined}) ->
  State;
maybe_start_timeout(State = #state{response_timeout = Timeout}) ->
%%  lager:notice("start response timeout"),
  ResponseTRef = erlang:send_after(Timeout, self(), response_timeout),
  State#state{response_tref = ResponseTRef, waiting_response = true}.

cancel_response_timeout(State = #state{response_tref = RTRef}) ->
%%  lager:notice("cancel response timeout"),
  catch (erlang:cancel_timer(RTRef)),
  State#state{response_tref = undefined, waiting_response = false}.

socket_active(Socket) ->
  inet:setopts(Socket, [{active, once}]).

connect(#state{ip = Ip, port = Port, connector = Reconnector, packet = Packet} = State) ->
  connection_registry:connecting(),
  case
    gen_tcp:connect(binary_to_list(Ip), Port, ?SOCKOPTS++[{packet, Packet}]) of
    {ok, Socket} ->
      connection_registry:connected(),
      State#state{socket = Socket, connected = true, connector = faxe_backoff:reset(Reconnector)};
    {error, Reason} ->
      connection_registry:disconnected(),
      lager:warning("connection to ~p ~p failed with reason: ~p", [Ip, Port, Reason]),
      {ok, Recon} = faxe_backoff:execute(Reconnector, reconnect),
      State#state{connector = Recon, connected = false}

  end.


