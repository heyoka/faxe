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
-export([init/3, process/3, options/0, handle_info/2, shutdown/1, metrics/0]).

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
  prev_crc32,
  fn_id
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

-define(RECON_MIN_INTERVAL, 100).
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

metrics() ->
  [
    {?METRIC_BYTES_READ, meter, []}
  ].

init(NodeId, _Ins,
    #{ip := Ip, port := Port, as := As, extract := Extract, changed := Changed,
      line_delimiter := Delimit, parser := Parser, min_length := MinL}) ->
  Reconnector = faxe_backoff:new({?RECON_MIN_INTERVAL, ?RECON_MAX_INTERVAL, ?RECON_MAX_RETRIES}),
  {ok, Reconnector1} = faxe_backoff:execute(Reconnector, do_reconnect),
  reconnect_watcher:new(20000, 15, io_lib:format("~s:~p ~p",[Ip, Port, ?MODULE])),
  connection_registry:reg(NodeId, Ip, Port, <<"tcp">>),
  {ok, all,
    #state{ip = Ip, port = Port, as = As, extract = Extract, parser = Parser, min_length = MinL,
      reconnector = Reconnector1, line_delimiter = Delimit, changes = Changed, fn_id = NodeId}}.

process(_In, #data_batch{points = _Points} = _Batch, State = #state{}) ->
  {ok, State};
process(_Inport, #data_point{} = _Point, State = #state{}) ->
  {ok, State}.

%% ignore lines that are less than min_length bytes long
handle_info({tcp, Socket, Data}, State=#state{min_length = Min}) when byte_size(Data) < Min ->
  inet:setopts(Socket, [{active, once}]),
  lager:info("message dropped, too short: ~p :: ~p",[byte_size(Data), Data]),
  {ok, State};
handle_info({tcp, Socket, Data0}, State=#state{fn_id = FNId}) ->
  Data = string:chomp(Data0),
%%  lager:notice("NewData: ~p", [Data]),
  node_metrics:metric(?METRIC_ITEMS_IN, 1, FNId),
  node_metrics:metric(?METRIC_BYTES_READ, faxe_util:bytes(Data), FNId),
  inet:setopts(Socket, [{active, once}]),
  maybe_emit(Data, State);
handle_info({tcp_closed, _S}, S=#state{}) ->
  connection_registry:disconnected(),
  try_reconnect(S#state{socket = undefined});
handle_info({tcp_error, Socket, _E}, State) ->
  lager:warning("tcp_error: ~p", [_E]),
  inet:setopts(Socket, [{active, once}]),
  {ok, State};
handle_info(do_reconnect, State=#state{ip = Ip, port = Port, line_delimiter = LD, reconnector = Recon}) ->
  connection_registry:connecting(),
  case connect(Ip, Port, LD) of
    {ok, Socket} ->
      connection_registry:connected(),
      inet:setopts(Socket, [{active, once}]),
      {ok, State#state{socket = Socket, reconnector = faxe_backoff:reset(Recon)}};
    {error, Error} ->
      lager:warning("[~p] Error connecting to ~p: ~p",[?MODULE, {Ip, Port},Error]),
      try_reconnect(State)
  end;
handle_info(_R, S) ->
  {ok, S}.

shutdown(#state{socket = Sock, timer_ref = Timer}) ->
  catch (erlang:cancel_timer(Timer)),
  catch (gen_tcp:close(Sock)).

try_reconnect(State=#state{reconnector = Reconnector}) ->
  case faxe_backoff:execute(Reconnector, do_reconnect) of
    {ok, Reconnector1} ->
      {ok, State#state{reconnector = Reconnector1}};
    {warning, Reason, Reconnector2} ->
      lager:warning("Reconnector warning: ~p", [Reason]),
      {ok, State#state{reconnector = Reconnector2}};
    {stop, Error} -> logger:error("[Client: ~p] reconnect error: ~p!",[?MODULE, Error]),
      {stop, {shutdown, Error}, State}
  end.

connect(Ip, Port, _LineDelimiter) ->
  reconnect_watcher:bump(),
  gen_tcp:connect(binary_to_list(Ip), Port, ?SOCKOPTS).

maybe_emit(Data, State = #state{changes = false}) -> do_emit(Data, State);
maybe_emit(Data, State = #state{changes = true, prev_crc32 = undefined}) ->
  DataCheckSum = erlang:crc32(Data),
  NewState = State#state{prev_crc32 = DataCheckSum},
  do_emit(Data, NewState);
maybe_emit(Data, State = #state{changes = true, prev_crc32 = PrevCheckSum}) ->
  DataCheckSum = erlang:crc32(Data),
  NewState = State#state{prev_crc32 = DataCheckSum},
  case  DataCheckSum /= PrevCheckSum of
    true -> do_emit(Data, NewState);
    false -> {ok, NewState}
  end.

do_emit(Data, State = #state{as = As, parser = Parser, extract = _Extract, fn_id = FNId}) ->
  case (catch binary_msg_parser:convert(Data, As, Parser)) of
    P when is_record(P, data_point) ->
      node_metrics:metric(?METRIC_ITEMS_OUT, 1, FNId),
      {emit, {1, P}, State};
    Err ->
      lager:warning("Parsing error [~p] ~nmessage ~p",[Parser, Err]),
      {ok, State}
  end.


