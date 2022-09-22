%%%-------------------------------------------------------------------
%%% @author heyoka
%%% @copyright (C) 2022
%%% @doc
%%% This node sends incoming data-items as json over udp
%%% @end
%%% Created : Sept 21. 2022 16:56
%%%-------------------------------------------------------------------
-module(esp_udp_send).
-author("heyoka").

%% API
-behavior(df_component).

-include("faxe.hrl").
%% API
-export([init/3, process/3, options/0, shutdown/1, metrics/0, handle_info/2]).

-define(RECONNECT_TIME, 2000).

-record(state, {
  peer,
  port,
  socket,
  as,
  fn_id
}).

-define(SOCKOPTS, [binary, {broadcast, false}]).

options() -> [
  {host, string, undefined},
  {port, integer}
].

metrics() ->
  [
    {?METRIC_BYTES_READ, meter, []}
  ].

init(NodeId, _Ins, #{port := Port, host := Host}) ->
  State = #state{port = Port, fn_id = NodeId, peer = binary_to_list(Host)},
  reconnect(0),
  {ok, all, State}.

process(_In, _DataItem, State = #state{socket = undefined}) ->
  lager:warning("got data-item but udp socket not setup yet"),
  {ok, State};
process(_In, DataItem, State = #state{socket = Socket, peer = Peer, port = Port}) ->
  Data = flowdata:to_json(DataItem),
  case catch gen_udp:send(Socket, Peer, Port, Data) of
    ok -> ok;
    Err -> lager:warning("error when sending udp packet: ~p",[Err, {Socket, Peer, Port, Data}])
  end,
  {ok, State}.

handle_info(connect, State) ->
  connect(State);
handle_info(_, State) ->
  {ok, State}.

shutdown(#state{socket = Sock}) ->
  catch (gen_udp:close(Sock)).

connect(S = #state{port = _Port}) ->
  Socket =
    case gen_udp:open(0, ?SOCKOPTS) of
      {ok, Sock} -> Sock;
      {error, _What} ->
        reconnect(?RECONNECT_TIME),
        undefined
    end,
  {ok, S#state{socket = Socket}}.

reconnect(T) ->
  erlang:send_after(T, self(),  connect).





