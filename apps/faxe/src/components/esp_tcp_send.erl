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
  connector,
  interval,
  count,
  prefix,
  timer_ref,
  msg_text,
  msg_json
}).

-define(SOCKOPTS, [{active, once}, binary]).

options() -> [
  {ip, binary},
  {port, integer},
  {every, duration, undefined},
  {msg_text, string, undefined},
  {msg_json, string, undefined}].

check_options() ->
  [{one_of_params, [msg_text, msg_json]}]
.

init(NodeId, _Ins,
    #{ip := Ip, port := Port, every := Dur, msg_text := Text, msg_json := Json}) ->

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
      connector = Recon,
      interval = Interval,
      msg_text = Text,
      msg_json = Json}}.

process(_In, #data_batch{points = _Points} = _Batch, State = #state{}) ->
  send(State),
  {ok, State};
process(_Inport, #data_point{} = _Point, State = #state{}) ->
  send(State),
  {ok, State}.

%% response
handle_info(reconnect, State = #state{}) ->
  connection_registry:connecting(),
  NewState = connect(State),
  {noreply, NewState};
handle_info({tcp, Socket, Data0}, State=#state{socket = _Socket, interval = _Interval, prefix = Prefix}) ->
  Data = binary_to_term(Data0),
%%  lager:notice("Data got from TCP:  ~p",[Data]),
  dataflow:emit(convert(#data_point{ts = faxe_time:now()}, Data, Prefix)),
  inet:setopts(Socket, [{active, once}]),
  NewState = poll(State),
  {ok, NewState};
handle_info(poll, State=#state{}) ->
  _Ret = send(State),
  {ok, State};
handle_info({tcp_closed, _S}, S=#state{connector = Reconnector, timer_ref = TRef}) ->
  connection_registry:disconnected(),
  catch (erlang:cancel_timer(TRef)),
  {ok, Recon} = faxe_backoff:execute(Reconnector, reconnect),
  NewState = poll(S#state{connector = Recon}),
  {ok, NewState};
handle_info({tcp_error, Socket, _}, S=#state{timer_ref = Timer}) ->
  catch (erlang:cancel_timer(Timer)),
  NewState = poll(S),
  inet:setopts(Socket, [{active, once}]),
  {ok, NewState};
handle_info(E, S) ->
  io:format("unexpected: ~p~n", [E]),
  {ok, S}.

shutdown(#state{socket = Sock, timer_ref = Timer}) ->
  catch (erlang:cancel_timer(Timer)),
  catch (gen_tcp:close(Sock)).

send(#state{msg_json = undefined, msg_text = Text} = State) ->
  do_send(Text, State);
send(#state{msg_json = Json} = State) ->
  do_send(jiffy:encode(Json), State).

do_send(Msg, #state{socket = Socket}) ->
  Ret = gen_tcp:send(Socket, Msg),
  Ret.

poll(State = #state{interval = undefined}) ->
  State;
poll(State = #state{interval = Interval}) ->
  TRef = erlang:send_after(Interval, self(), poll),
  State#state{timer_ref = TRef}.

connect(#state{ip = Ip, port = Port, connector = Reconnector} = State) ->
  connection_registry:connecting(),
  case
    gen_tcp:connect(binary_to_list(Ip), Port, ?SOCKOPTS) of
    {ok, Socket} ->
      connection_registry:connected(),
      State#state{socket = Socket};
    {error, Reason} ->
      connection_registry:disconnected(),
      lager:warning("connection to ~p ~p failed with reason: ~p", [Ip, Port, Reason]),
      {ok, Recon} = faxe_backoff:execute(Reconnector, reconnect),
      State#state{connector = Recon}

  end.

convert(Point=#data_point{}, Data, NamePrefix) when is_list(Data) ->
  DataList = lists:zip(lists:seq(1, length(Data)), Data),
  F = fun({N, V}, P) -> flowdata:set_field(P, list_to_binary(lists:concat([NamePrefix, N])), V) end,
  lists:foldl(F, Point, DataList).

