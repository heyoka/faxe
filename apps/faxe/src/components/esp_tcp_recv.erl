%%%-------------------------------------------------------------------
%%% @author heyoka
%%% @copyright (C) 2019, <COMPANY>
%%% @doc
%%% This node connects to a tcp endpoint and awaits data in a special format, which is defined
%%% by the parser parameter, the parser will then try to convert the data to faxe format and emit the
%%% result
%%% At the moment tcp messages must start with a 2 byte header denoting the length of the following data.
%%% << Length_Header:16/integer, Data:{Length_Header}/binary >>
%%%
%%% The tcp listener is protected against flooding with the {active, once} inet option
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
  extract = false,
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
  {as, binary, <<"data">>}, %% alias for fieldname
  {extract, is_set}, %% overrides as
  {parser, atom, undefined} %% parser module to use
].


init(_NodeId, _Ins,
    #{ip := Ip, port := Port, as := As, parser := Parser, extract := Extract} = Opts) ->
  lager:notice("++++ ~p ~ngot opts: ~p ~n",[_NodeId, Opts]),
  Reconnector = modbus_reconnector:new(
    {?RECON_MIN_INTERVAL, ?RECON_MAX_INTERVAL, ?RECON_MAX_RETRIES}),
  {ok, Reconnector1} = modbus_reconnector:execute(Reconnector, do_reconnect),
  {ok, all,
    #state{ip = Ip, port = Port, as = As, extract = Extract,
      parser = Parser, reconnector = Reconnector1}}.

process(_In, #data_batch{points = _Points} = _Batch, State = #state{}) ->
  {ok, State};
process(_Inport, #data_point{} = _Point, State = #state{}) ->
  {ok, State}.

handle_info({tcp, Socket, Data}, State=#state{as = As, parser = Parser, extract = Extract}) ->
  %lager:notice("Data got from TCP:  ~p~nSize:~p at recbuf:~p",[Data, byte_size(Data), inet:getopts(Socket, [recbuf, buffer])]),
%%  P = convert(Data, As, Extract, Parser),
  P = tcp_msg_parser:convert(Data, As, Extract, Parser),
  lager:warning("~n to json: ~s",[flowdata:to_json(P)]),
  dataflow:emit(P),
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

connect(Ip, Port) ->
   gen_tcp:connect(binary_to_list(Ip), Port, ?SOCKOPTS).


