%%%-------------------------------------------------------------------
%%% @author heyoka
%%% @copyright (C) 2019, <COMPANY>
%%% @doc
%%% This node connects to a tcp endpoint and awaits data in a special format, which is defined
%%% by the parser parameter, the parser will then try to convert the data to faxe format and emit the
%%% result
%%% At the moment tcp messages must start with a 2 byte header denoting the length of the following data.
%%% << Length_Header:16/integer, Data:{Length_Header}/binary >>
%%% If the 'changes' option is given, the node will only emit on changed values
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
-export([init/3, process/3, options/0, handle_info/2, shutdown/1, metrics/0, check_options/0]).

-record(state, {
  ip,
  port,
  socket,
  as,
  extract = false,
  reconnector,
  parser = undefined :: undefined|atom(), %% parser module
  changes = false,
  prev_crc32,
  fn_id,
  packet = 2
}).

-define(SOCKOPTS,
  [
    {active, once},
    binary,
    {reuseaddr, true},
    {keepalive, true},
    {recbuf, 2048},
    {buffer, 4096}
  ]).

-define(RECON_MIN_INTERVAL, 100).
-define(RECON_MAX_INTERVAL, 10000).
-define(RECON_MAX_RETRIES, infinity).

options() -> [
  {ip, binary, undefined},
  {port, integer},
%%  {as, binary, <<"data">>}, %% alias for fieldname
  {as, binary, undefined}, %% alias for fieldname
  {extract, is_set}, %% overrides as
  {parser, string, undefined}, %% parser module to use
  {changed, is_set, false}, %% only emit, when new data is different to previous
  {packet, any, 2}
                      %% 1 | 2 | 4 | 'line'
                      %% Packets consist of a header specifying the number of bytes in the packet,
                      %% followed by that number of bytes. The header length can be one, two, or four bytes,
                      %% and containing an unsigned integer in big-endian byte order.
].

check_options() ->
  [{one_of, packet, [1, 2, 4, <<"line">>]},
    {func, parser,
      fun
        (undefined) -> true;
        (ParserName) -> esp_parser:parser_check(ParserName)
      end,
      <<" has not a valid parser-name">>
    }].

metrics() ->
  [
    {?METRIC_BYTES_READ, meter, []}
  ].

init(NodeId, _Ins,
    #{ip := Ip, port := Port, as := As, parser := Parser0, extract := Extract, changed := Changed, packet := Packet}) ->
  Reconnector = faxe_backoff:new(
    {?RECON_MIN_INTERVAL, ?RECON_MAX_INTERVAL, ?RECON_MAX_RETRIES}),
  Parser = case Parser0 of undefined -> undefined; _ -> faxe_util:save_binary_to_atom(Parser0) end,
  {ok, Reconnector1} = faxe_backoff:execute(Reconnector, do_reconnect),
  reconnect_watcher:new(20000, 15, io_lib:format("~p:~p ~p",[Ip, Port, ?MODULE])),
  connection_registry:reg(NodeId, Ip, Port, <<"tcp">>),
  {ok, all,
    #state{ip = Ip, port = Port, as = As, extract = Extract, changes = Changed,
      parser = Parser, reconnector = Reconnector1, fn_id = NodeId, packet = p_type(Packet)}}.

process(_In, #data_batch{points = _Points} = _Batch, State = #state{}) ->
  {ok, State};
process(_Inport, #data_point{} = _Point, State = #state{}) ->
  {ok, State}.


handle_info({tcp, Socket, Data0}, State=#state{fn_id = FNId, packet = Packet}) ->
  node_metrics:metric(?METRIC_ITEMS_IN, 1, FNId),
  node_metrics:metric(?METRIC_BYTES_READ, faxe_util:bytes(Data0), FNId),
  inet:setopts(Socket, [{active, once}]),
  %% if we use packet == line, then we strip the newline at the end, cause it will still be there
  Data = case Packet of
           line -> string:chomp(Data0);
           _ -> Data0
         end,
  maybe_emit(Data, State);
handle_info({tcp_closed, _S}, S=#state{}) ->
  connection_registry:disconnected(),
  try_reconnect(S#state{socket = undefined});
handle_info({tcp_error, Socket, _}, State) ->
  inet:setopts(Socket, [{active, once}]),
  {ok, State};
handle_info(do_reconnect, State=#state{ip = Ip, port = Port, packet = Packet, reconnector = Recon}) ->
  connection_registry:connecting(),
  case connect(Ip, Port, Packet) of
    {ok, Socket} ->
      connection_registry:connected(),
      inet:setopts(Socket, [{active, once}]),
      {ok, State#state{socket = Socket, reconnector = faxe_backoff:reset(Recon)}};
    {error, Error} -> lager:warning("[~p] Error connecting to ~p: ~p",[?MODULE, {Ip, Port},Error]),
      try_reconnect(State)
  end;
handle_info(_E, S) ->
  {ok, S}.

shutdown(#state{socket = Sock}) ->
  catch (gen_tcp:close(Sock)).

try_reconnect(State=#state{reconnector = Reconnector}) ->
  case faxe_backoff:execute(Reconnector, do_reconnect) of
    {ok, Reconnector1} ->
      {ok, State#state{reconnector = Reconnector1}};
    {stop, Error} -> logger:error("[Client: ~p] reconnect error: ~p!",[?MODULE, Error]),
      {stop, {shutdown, Error}, State}
  end.

connect(undefined, Port, Packet) ->
  {ok, Socket} = gen_tcp:listen(Port, ?SOCKOPTS++[{packet, Packet}]),
  %% this blocks the process until a connection comes in
  gen_tcp:accept(Socket);
connect(Ip, Port, Packet) ->
  reconnect_watcher:bump(),
  gen_tcp:connect(binary_to_list(Ip), Port, ?SOCKOPTS++[{packet, Packet}]).


maybe_emit(Data, State = #state{changes = false}) -> do_emit(Data, State);
maybe_emit(Data, State = #state{changes = true, prev_crc32 = undefined}) ->
  {_T, DataCheckSum} = timer:tc(erlang,crc32, [Data]),
  NewState = State#state{prev_crc32 = DataCheckSum},
  do_emit(Data, NewState);
maybe_emit(Data, State = #state{changes = true, prev_crc32 = PrevCheckSum}) ->
  {_T, DataCheckSum} = timer:tc(erlang, crc32, [Data]),
  NewState = State#state{prev_crc32 = DataCheckSum},
  case  DataCheckSum /= PrevCheckSum of
    true -> do_emit(Data, NewState);
    false -> {ok, NewState}
  end.

do_emit(Data, State = #state{as = undefined, parser = undefined}) ->
  Item = flowdata:from_json_struct(Data),
  {emit, {1, Item}, State};
do_emit(Data, State = #state{as = As, parser = Parser}) ->
  case (catch binary_msg_parser:convert(Data, As, Parser)) of
    P when is_record(P, data_point) ->
      {emit, {1, P}, State};
    Err ->
      lager:warning("Parsing error [~p] ~nmessage ~p",[Parser, Err]),
      {ok, State}
  end.



p_type(<<"line">>) -> line;
p_type(Other) -> Other.