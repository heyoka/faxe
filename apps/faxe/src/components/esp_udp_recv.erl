%%%-------------------------------------------------------------------
%%% @author heyoka
%%% @copyright (C) 2020
%%% @doc
%%% This node listens on a udp socket and awaits data in a special format, which is defined
%%% by the parser parameter, the parser will then try to convert the data to faxe format and emit the
%%% result
%%%
%%% If the 'changes' option is given, the node will only emit on changed values
%%%
%%% The udp listener is protected against flooding with the {active, once} inet option.
%%% @end
%%% Created : March 12. 2020 20:33
%%%-------------------------------------------------------------------
-module(esp_udp_recv).
-author("heyoka").

%% API
-behavior(df_component).

-include("faxe.hrl").
%% API
-export([init/3, process/3, options/0, handle_info/2, shutdown/1, metrics/0]).

-record(state, {
  port,
  socket,
  as,
  parser = undefined :: undefined|atom(), %% parser module
  changes = false,
  prev_crc32,
  fn_id
}).

-define(SOCKOPTS,
  [
    {ip, {127,0,0,1}},
    {active, once},
    binary,
    {reuseaddr, true},
    {recbuf, 2048},
    {buffer, 4096}
  ]).

options() -> [
  {port, integer},
  {as, binary, <<"data">>}, %% alias for fieldname
  {parser, atom, undefined}, %% parser module to use
  {changed, is_set, false} %% only emit, when new data is different to previous
].

metrics() ->
  [
    {?METRIC_BYTES_READ, meter, []}
  ].


init(NodeId, _Ins,
    #{port := Port, as := As, parser := Parser, changed := Changed}) ->
  State = #state{port = Port, as = As, changes = Changed, parser = Parser, fn_id = NodeId},
  NewState = connect(State),
  {ok, all, NewState}.

process(_In, #data_batch{points = _Points} = _Batch, State = #state{}) ->
  {ok, State};
process(_Inport, #data_point{} = _Point, State = #state{}) ->
  {ok, State}.

handle_info({udp, Socket, _IP, _InPortNo, Packet}, State=#state{fn_id = FNId}) ->
%%  lager:notice("udp packet arrived: ~p", [Packet]),
  node_metrics:metric(?METRIC_ITEMS_IN, 1, FNId),
  node_metrics:metric(?METRIC_BYTES_SENT, faxe_util:bytes(Packet), FNId),
  inet:setopts(Socket, [{active, once}]),
  maybe_emit(Packet, State);
handle_info(_E, S) ->
  {ok, S}.

shutdown(#state{socket = Sock}) ->
  catch (gen_udp:close(Sock)).

connect(S = #state{port = Port}) ->
  Socket = gen_udp:open(Port, ?SOCKOPTS),
  S#state{socket = Socket}.

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
    false -> {ok, State}
  end.

do_emit(Data, State = #state{as = As, parser = undefined}) ->
  %% just get it from json
  DataItem = flowdata:from_json_struct(Data),
  {emit, DataItem, State};
do_emit(Data, State = #state{as = As, parser = Parser}) ->
  %% use parser to retrieve internal data structure
  case (catch binary_msg_parser:convert(Data, As, Parser)) of
    P when is_record(P, data_point) -> {emit, P, State};
    Err -> lager:warning("Parsing error [~p] ~nmessage ~p",[Parser, Err]), {ok, State}
  end.



