%%%-------------------------------------------------------------------
%%% @author heyoka
%%% @copyright (C) 2019, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 27. May 2019 09:00
%%%-------------------------------------------------------------------
-module(esp_tcp_recv).
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
  as,
  timer_ref,
  reconnector,
  parser = undefined :: undefined|atom() %% parser module
}).

-define(SOCKOPTS,
  [
    {active, once},
    binary,
    {reuseaddr, true},
    {packet, 2},
    {keepalive, true},
    {recbuf, 2048},
    {buffer, 4096}
  ]).

-define(RECON_MIN_INTERVAL, 1).
-define(RECON_MAX_INTERVAL, 10).
-define(RECON_MAX_RETRIES, infinity).

options() -> [
  {ip, binary}, {port, integer},
  {as, binary, <<"value">>}, %% alias for fieldname
  {parser, atom, undefined} %% parser module to use
].


init(_NodeId, _Ins,
    #{ip := Ip, port := Port, as := As, parser := Parser}=Opts) ->
  lager:notice("++++ ~p ~ngot opts: ~p ~n",[_NodeId, Opts]),
  Reconnector = modbus_reconnector:new({?RECON_MIN_INTERVAL, ?RECON_MAX_INTERVAL, ?RECON_MAX_RETRIES}),
  {ok, Reconnector1} = modbus_reconnector:execute(Reconnector, do_reconnect),
  {ok, all,
    #state{ip = Ip, port = Port, as = As, parser = Parser, reconnector = Reconnector1}}.

process(_In, #data_batch{points = _Points} = _Batch, State = #state{}) ->
  {ok, State};
process(_Inport, #data_point{} = _Point, State = #state{}) ->
  {ok, State}.

handle_info({tcp, Socket, Data}, State=#state{as = As, parser = Parser}) ->
  lager:notice("Data got from TCP:  ~p~nSize:~p at recbuf:~p",[Data, byte_size(Data), inet:getopts(Socket, [recbuf, buffer])]),
  dataflow:emit(convert(Data, As, Parser)),
%%  dataflow:emit(convert(#data_point{ts = faxe_time:now()}, Data, As)),
  inet:setopts(Socket, [{active, once}]),
  {ok, State};
handle_info({tcp_closed, _S}, S=#state{}) ->
  try_reconnect(S#state{socket = undefined});
handle_info({tcp_error, Socket, _}, State) ->
  inet:setopts(Socket, [{active, once}]),
  {ok, State};
handle_info(do_reconnect, State=#state{ip = Ip, port = Port}) ->
  case connect(Ip, Port) of
    {ok, Socket} -> inet:setopts(Socket, [{active, once}]), {ok, State#state{socket = Socket}};
    {error, Error} -> lager:error("[~p] Error when connection: ~p",[?MODULE, Error]), try_reconnect(State)
  end;
handle_info(E, S) ->
  io:format("unexpected: ~p~n", [E]),
  {ok, S}.

shutdown(#state{socket = Sock, timer_ref = Timer}) ->
  catch (erlang:cancel_timer(Timer)),
  catch (gen_tcp:close(Sock)).

try_reconnect(State=#state{reconnector = Reconnector}) ->
  case modbus_reconnector:execute(Reconnector, do_reconnect) of
    {ok, Reconnector1} ->
      {ok, State#state{reconnector = Reconnector1}};
    {stop, Error} -> logger:error("[Client: ~p] reconnect error: ~p!",[?MODULE, Error]),
      {stop, {shutdown, Error}, State}
  end.

connect(Ip, Port) ->
   gen_tcp:connect(binary_to_list(Ip), Port, ?SOCKOPTS).

%% @doc parse and convert binary-data
convert(Data, As, undefined) ->
  NewPoint = flowdata:set_field(#data_point{ts = faxe_time:now()}, As, Data),
  lager:notice("[~p] new point: ~p",[?MODULE, NewPoint]),
  NewPoint;
convert(Data, As, Parser) ->
  {T, Json} = timer:tc(Parser, parse, [Data]),
  lager:notice("[~p] parser time: ~p",[?MODULE, T]),
  convert(Json, As, undefined).


