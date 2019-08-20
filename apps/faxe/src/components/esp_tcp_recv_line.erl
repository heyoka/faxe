%%%-------------------------------------------------------------------
%%% @author heyoka
%%% @copyright (C) 2019, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 27. May 2019 09:00
%%%-------------------------------------------------------------------
-module(esp_tcp_recv_line).
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
  line_delimiter,
  reconnector,
  parser = undefined :: undefined|atom(), %% parser module
  min_length
}).

-define(SOCKOPTS,
  [
    {active, once},
    binary,
    {reuseaddr, true},
    {packet, line},
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
  {line_delimiter, binary, $\n}, %% not used at the moment
  {parser, atom, undefined}, %% parser module to use
  {min_length, integer, 61} %% lines shorter than min_length bytes will be ignored
].


init(_NodeId, _Ins,
    #{ip := Ip, port := Port, as := As, line_delimiter := Delimit, parser := Parser, min_length := MinL}=Opts) ->
  lager:notice("++++ ~p ~ngot opts: ~p ~n",[_NodeId, Opts]),
  Reconnector = modbus_reconnector:new({?RECON_MIN_INTERVAL, ?RECON_MAX_INTERVAL, ?RECON_MAX_RETRIES}),
  {ok, Reconnector1} = modbus_reconnector:execute(Reconnector, do_reconnect),
  {ok, all,
    #state{ip = Ip, port = Port, as = As, parser = Parser, min_length = MinL,
      reconnector = Reconnector1, line_delimiter = Delimit}}.

process(_In, #data_batch{points = _Points} = _Batch, State = #state{}) ->
  {ok, State};
process(_Inport, #data_point{} = _Point, State = #state{}) ->
  {ok, State}.

%% ignore lines that are less than 60 bytes long
handle_info({tcp, Socket, Data}, State=#state{min_length = Min}) when byte_size(Data) < Min ->
  inet:setopts(Socket, [{active, once}]),
  lager:notice("message dropped: ~p :: ~p",[byte_size(Data), Data]),
  {ok, State};
handle_info({tcp, Socket, Data0}, State=#state{as = As, parser = Parser}) ->
  Data = string:chomp(Data0),
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
handle_info(do_reconnect, State=#state{ip = Ip, port = Port, line_delimiter = LD}) ->
  case connect(Ip, Port, LD) of
    {ok, Socket} -> inet:setopts(Socket, [{active, once}]), {ok, State#state{socket = Socket}};
    {error, Error} -> lager:error("[~p] Error connecting: ~p",[?MODULE, Error]), try_reconnect(State)
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

connect(Ip, Port, _LineDelimiter) ->
   gen_tcp:connect(binary_to_list(Ip), Port, ?SOCKOPTS).

%% @doc parse and convert binary-data
convert(Data, As, undefined) ->
  NewPoint = flowdata:set_field(#data_point{ts = faxe_time:now()}, As, Data),
  lager:notice("[~p] new point: ~p",[?MODULE, NewPoint]),
  PP = flowdata:expand_json_field(NewPoint, As),
  lager:warning("[~p] expanded json: ~p",[PP]),
  NewPoint;
convert(Data, As, Parser) ->
  {T, Json} = timer:tc(Parser, parse, [Data]),
  lager:notice("[~p] parser time: ~p",[?MODULE, T]),
  convert(Json, As, undefined).


