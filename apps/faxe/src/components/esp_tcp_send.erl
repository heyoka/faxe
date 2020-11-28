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
-export([init/3, process/3, options/0, handle_info/2, shutdown/1, check_options/0]).

-record(state, {
  ip,
  port,
  socket,
  socket_active = once,
  packet,
  response_as,
  connector,
  connected,
  interval,
  count,
  prefix,
  timer_ref,
  msg_text,
  msg_json,
  last_item
}).

-define(SOCKOPTS, [{active, once}, binary]).

options() -> [
  {ip, binary},
  {port, integer},
  {packet, integer, 2},
  {every, duration, undefined},
  {response_as, string, undefined},
  {msg_text, string, undefined},
  {msg_json, string, undefined}].

check_options() ->
  [
%%    {one_of_params, [msg_text, msg_json]}
  ]
.

init(NodeId, _Ins,
    #{ip := Ip, port := Port, every := Dur,
      msg_text := Text, msg_json := Json, response_as := RespAs, packet := Packet}) ->

  connection_registry:reg(NodeId, Ip, Port, <<"tcp">>),

  Interval =
    case Dur of
      undefined -> undefined;
      _ -> faxe_time:duration_to_ms(Dur)
    end,
  Reconnector = faxe_backoff:new({10, 1200}),
  {ok, Recon} = faxe_backoff:execute(Reconnector, reconnect),

  {ok, all,
    #state{
      ip = Ip,
      port = Port,
      packet = Packet,
      response_as = RespAs,
      connector = Recon,
      interval = Interval,
      msg_text = Text,
      msg_json = Json}}.

process(_In, Item, State = #state{connected = false}) ->
  {ok, State#state{last_item = Item}};
process(_In, Item, State = #state{}) ->
  NewState = State#state{last_item = Item},
  send(NewState),
  {ok, NewState}.

%% response
handle_info(reconnect, State = #state{}) ->
  connection_registry:connecting(),
  NewState = connect(State),
  {ok, NewState};
handle_info({tcp, _Socket, _Data0}, State=#state{response_as = undefined}) ->
  lager:notice("got tcp data: ~p, but response will be ignored",[_Data0]),
  {ok, State};
handle_info({tcp, Socket, Data0}, State=#state{socket = _Socket, interval = _Interval, response_as = As}) ->
  lager:notice("Data got from TCP:  ~p",[Data0]),
  P = flowdata:set_field(#data_point{ts = faxe_time:now()}, As, Data0),
  inet:setopts(Socket, [{active, once}]),
  NewState = sender(State),
  {emit, {1, P}, NewState};
handle_info(send, State=#state{}) ->
  _Ret = send(State),
  {ok, State};
handle_info({tcp_closed, _S}, S=#state{connector = Reconnector, timer_ref = TRef}) ->
  connection_registry:disconnected(),
  catch (erlang:cancel_timer(TRef)),
  {ok, Recon} = faxe_backoff:execute(Reconnector, reconnect),
  NewState = sender(S#state{connector = Recon, connected = false}),
  {ok, NewState};
handle_info({tcp_error, Socket, _}, S=#state{timer_ref = Timer}) ->
  catch (erlang:cancel_timer(Timer)),
  NewState = sender(S),
  inet:setopts(Socket, [{active, once}]),
  {ok, NewState};
handle_info(E, S) ->
  io:format("unexpected: ~p~n", [E]),
  {ok, S}.

shutdown(#state{socket = Sock, timer_ref = Timer}) ->
  catch (erlang:cancel_timer(Timer)),
  catch (gen_tcp:close(Sock)).

send(#state{msg_json = undefined, msg_text = undefined, last_item = LastItem} = State) ->
  lager:notice("send: ~p",[LastItem]),
  Msg = flowdata:to_json(LastItem),
  do_send(Msg, State);
send(#state{msg_json = undefined, msg_text = Text} = State) ->
  lager:notice("send: ~p",[Text]),
  do_send(Text, State);
send(#state{msg_json = Json} = State) ->
  do_send(jiffy:encode(Json), State).

do_send(Msg, #state{socket = Socket}) ->
  Ret = gen_tcp:send(Socket, Msg),
  Ret.

sender(State = #state{interval = undefined}) ->
  State;
sender(State = #state{interval = Interval}) ->
  TRef = erlang:send_after(Interval, self(), send),
  State#state{timer_ref = TRef}.

connect(#state{ip = Ip, port = Port, connector = Reconnector, packet = Packet} = State) ->
  connection_registry:connecting(),
  case
    gen_tcp:connect(binary_to_list(Ip), Port, ?SOCKOPTS++[{packet, Packet}]) of
    {ok, Socket} ->
      connection_registry:connected(),
      State#state{socket = Socket, connected = true};
    {error, Reason} ->
      connection_registry:disconnected(),
      lager:warning("connection to ~p ~p failed with reason: ~p", [Ip, Port, Reason]),
      {ok, Recon} = faxe_backoff:execute(Reconnector, reconnect),
      State#state{connector = Recon, connected = false}

  end.


