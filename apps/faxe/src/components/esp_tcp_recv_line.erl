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
  extract = false,
  timer_ref,
  line_delimiter,
  reconnector,
  parser = undefined :: undefined|atom(), %% parser module
  min_length,
  changes = false,
  prev_crc32
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

-define(RECON_MIN_INTERVAL, 200).
-define(RECON_MAX_INTERVAL, 7000).
-define(RECON_MAX_RETRIES, infinity).

options() -> [
  {ip, binary}, {port, integer},
  {as, binary, <<"data">>}, %% alias for fieldname
  {extract, is_set}, %% extract the parsed message into the fields list of data_point{} ?
  %% "extract" will always override "as"
  {line_delimiter, binary, $\n}, %% not used at the moment
  {parser, atom, undefined}, %% parser module to use
  {min_length, integer, 61}, %% lines shorter than min_length bytes will be ignored
  {changed, is_set}
].


init(_NodeId, _Ins,
    #{ip := Ip, port := Port, as := As, extract := Extract, changed := Changed,
      line_delimiter := Delimit, parser := Parser, min_length := MinL}) ->
  Reconnector = faxe_backoff:new({?RECON_MIN_INTERVAL, ?RECON_MAX_INTERVAL, ?RECON_MAX_RETRIES}),
  {ok, Reconnector1} = faxe_backoff:execute(Reconnector, do_reconnect),
  {ok, all,
    #state{ip = Ip, port = Port, as = As, extract = Extract, parser = Parser, min_length = MinL,
      reconnector = Reconnector1, line_delimiter = Delimit, changes = Changed}}.

process(_In, #data_batch{points = _Points} = _Batch, State = #state{}) ->
  {ok, State};
process(_Inport, #data_point{} = _Point, State = #state{}) ->
  {ok, State}.

%% ignore lines that are less than 60 bytes long
handle_info({tcp, Socket, Data}, State=#state{min_length = Min}) when byte_size(Data) < Min ->
  inet:setopts(Socket, [{active, once}]),
  lager:notice("message dropped: ~p :: ~p",[byte_size(Data), Data]),
  {ok, State};
handle_info({tcp, Socket, Data0}, State=#state{}) ->
  Data = string:chomp(Data0),
%%  lager:notice("NewData: ~p", [Data]),
  NewState = maybe_emit(Data, State),
  inet:setopts(Socket, [{active, once}]),
  {ok, NewState};
handle_info({tcp_closed, _S}, S=#state{}) ->
  lager:notice("tcp_closed"),
  try_reconnect(S#state{socket = undefined});
handle_info({tcp_error, Socket, _}, State) ->
  inet:setopts(Socket, [{active, once}]),
  {ok, State};
handle_info(do_reconnect, State=#state{ip = Ip, port = Port, line_delimiter = LD}) ->
  case connect(Ip, Port, LD) of
    {ok, Socket} -> lager:notice("connected to ~p" ,[Ip] ),
      inet:setopts(Socket, [{active, once}]), {ok, State#state{socket = Socket}};
    {error, Error} -> lager:warning("[~p] Error connecting to ~p: ~p",[?MODULE, {Ip, Port},Error]), try_reconnect(State)
  end;
handle_info(_E, S) ->
  {ok, S}.

shutdown(#state{socket = Sock, timer_ref = Timer}) ->
  catch (erlang:cancel_timer(Timer)),
  catch (gen_tcp:close(Sock)).

try_reconnect(State=#state{reconnector = Reconnector}) ->
  case faxe_backoff:execute(Reconnector, do_reconnect) of
    {ok, Reconnector1} ->
      {ok, State#state{reconnector = Reconnector1}};
    {stop, Error} -> logger:error("[Client: ~p] reconnect error: ~p!",[?MODULE, Error]),
      {stop, {shutdown, Error}, State}
  end.

connect(Ip, Port, _LineDelimiter) ->
  lager:notice("connect to: ~p",[{Ip, Port}]),
  gen_tcp:connect(binary_to_list(Ip), Port, ?SOCKOPTS).

maybe_emit(Data, State = #state{changes = false}) -> do_emit(Data, State);
maybe_emit(Data, State = #state{changes = true, prev_crc32 = undefined}) ->
  {_T, DataCheckSum} = timer:tc(erlang,crc32, [Data]),
  NewState = State#state{prev_crc32 = DataCheckSum},
  do_emit(Data, NewState);
maybe_emit(Data, State = #state{changes = true, prev_crc32 = PrevCheckSum}) ->
  {_T, DataCheckSum} = timer:tc(erlang,crc32, [Data]),
  NewState = State#state{prev_crc32 = DataCheckSum},
  case  DataCheckSum /= PrevCheckSum of
    true -> do_emit(Data, NewState);
    false -> NewState
  end.

do_emit(Data, State = #state{as = As, parser = Parser, extract = _Extract}) ->
  case (catch tcp_msg_parser:convert(Data, As, Parser)) of
    P when is_record(P, data_point) -> dataflow:emit(P);
    Err -> lager:warning("Parsing error [~p] ~nmessage ~p",[Parser, Err])
  end,
  State.


