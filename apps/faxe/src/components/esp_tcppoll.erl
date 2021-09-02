%%%-------------------------------------------------------------------
%%% @author heyoka
%%% @copyright (C) 2019, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 27. May 2019 09:00
%%%-------------------------------------------------------------------
-module(esp_tcppoll).
-author("heyoka").

%% API
-behavior(df_component).

-include("faxe.hrl").
%% API
-export([init/3, process/3, options/0, handle_info/2, shutdown/1]).

-record(state, {
  ip,
  port,
  socket,
  interval,
  count,
  prefix,
  timer_ref
}).

-define(SOCKOPTS, [{active, once}, binary]).

options() -> [
  {ip, binary},
  {port, integer},
  {every, duration, "1s"},
  {count, integer, 1},
  {prefix, string, <<"val_">>}].

init(NodeId, _Ins, #{ip := Ip, port := Port, every := Dur, count := C, prefix := Prefix}) ->
  connection_registry:reg(NodeId, Ip, Port, <<"tcp">>),
  {ok, Socket} = connect(Ip, Port),
  Interval = faxe_time:duration_to_ms(Dur),
  TRef = poll(0),
  {ok, all,
    #state{ip = Ip, port = Port, socket = Socket, interval = Interval,
      timer_ref = TRef, count = C, prefix = binary_to_list(Prefix)}}.

process(_In, #data_batch{points = _Points} = _Batch, State = #state{}) ->
  {ok, State};
process(_Inport, #data_point{} = _Point, State = #state{}) ->
  {ok, State}.

handle_info({tcp, Socket, Data0}, State=#state{socket = _Socket, interval = Interval, prefix = Prefix}) ->
  Data = binary_to_term(Data0),
%%  lager:notice("Data got from TCP:  ~p",[Data]),
  dataflow:emit(convert(#data_point{ts = faxe_time:now()}, Data, Prefix)),
  inet:setopts(Socket, [{active, once}]),
  TRef = poll(Interval),
  {ok, State#state{timer_ref = TRef}};
handle_info(poll, State=#state{socket = S, count = C}) ->
  gen_tcp:send(S, [<<"get">>, C]),
  {ok, State};
handle_info({tcp_closed, _S}, S=#state{ip = Ip, port = Port, interval = Interval, timer_ref = TRef}) ->
  connection_registry:disconnected(),
  catch (erlang:cancel_timer(TRef)),
  {ok, Socket} = connect(Ip, Port),
  poll(Interval),
  {ok, S#state{socket = Socket}};
handle_info({tcp_error, Socket, _}, S=#state{interval = Interval, timer_ref = Timer}) ->
  catch (erlang:cancel_timer(Timer)),
  TRef = poll(Interval),
  inet:setopts(Socket, [{active, once}]),
  {ok, S#state{timer_ref = TRef}};
handle_info(E, S) ->
  io:format("unexpected: ~p~n", [E]),
  {ok, S}.

shutdown(#state{socket = Sock, timer_ref = Timer}) ->
  catch (erlang:cancel_timer(Timer)),
  catch (gen_tcp:close(Sock)).

poll(Interval) ->
  erlang:send_after(Interval, self(), poll).

connect(Ip, Port) ->
  connection_registry:connected(),
  gen_tcp:connect(binary_to_list(Ip), Port, ?SOCKOPTS).

convert(Point=#data_point{}, Data, NamePrefix) when is_list(Data) ->
  DataList = lists:zip(lists:seq(1, length(Data)), Data),
  F = fun({N, V}, P) -> flowdata:set_field(P, list_to_binary(lists:concat([NamePrefix, N])), V) end,
  lists:foldl(F, Point, DataList).

