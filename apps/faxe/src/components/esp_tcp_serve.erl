%%%-------------------------------------------------------------------
%%% @author heyoka
%%% @copyright (C) 2023
%%% @doc
%%%
%%% @end
%%% Created : 19. April 2023 09:00
%%%-------------------------------------------------------------------
-module(esp_tcp_serve).
-author("heyoka").

%% API
-behavior(df_component).

-include("faxe.hrl").
%% API
-export([init/3, process/3, options/0, handle_info/2, shutdown/1, check_options/0, metrics/0]).

-record(state, {
  port,
  packet,
  listeners,
  ranch_sup_pid,
  open,
  last_item,
  format,
  field,
  fn_id
}).


options() -> [
  {port, integer},
  {packet, any, 2}, %% 1 | 2 | 3 | 'line'
  {format, string, <<"json">>}, %% 'json' | 'raw' | 'etf' | 'msgpack'
  {field, string, undefined}
].

check_options() ->
  [
    {one_of, packet, [1, 2, 4, <<"line">>]}
  ]
.

metrics() ->
  [
    {?METRIC_BYTES_SENT, meter, []}
  ].

init(NodeId, _Ins, #{port := Port, packet := Packet0, format := Format, field := Field} = Opts) ->
  MaxConns = 3,
  connection_registry:reg(NodeId, <<"0.0.0.0">>, Port, <<"tcp">>),
%%  lager:info("opts: ~p",[Opts]),
  Packet = p_type(Packet0),
  RanchOpts = #{socket_opts => [{port, Port}], max_connections => MaxConns, num_acceptors => 1},
  {ok, SPid} = ranch:start_listener(NodeId, ranch_tcp, RanchOpts, faxe_tcp_server,
    #{parent => self(), tcp_opts => [{active, once}, binary , {sndbuf, 2048}, {packet, Packet}]}
  ),

  {ok, all,
    #state{
      ranch_sup_pid = SPid,
      listeners = sets:new([{version, 2}]),
      port = Port,
      packet = Packet,
      last_item = flowdata:new(),
      fn_id = NodeId,
      format = Format,
      field = Field
    }
  }.

process(_In, Item, State = #state{open = false}) ->
%%  lager:notice("peer not connected yet, drop item"),
  {ok, State#state{last_item = Item}};
process(_In, Item, State = #state{listeners = Servers, packet = Packet}) ->
  NewState = State#state{last_item = Item},
  Data0 = build(Item, State),
  Data =
  case Packet of
    line -> <<Data0/binary, "\r\n">>;
    _ -> Data0
  end,
%%  lager:info("send to servers: ~p",[sets:to_list(Servers)]),
  [Server ! {data, Data} || Server <- sets:to_list(Servers)],
  {ok, NewState}.

%% response
handle_info({tcp_server_up, ServerPid}, State=#state{listeners = Servers}) ->
  erlang:monitor(process, ServerPid),
  NewServers = sets:add_element(ServerPid, Servers),
  {ok, State#state{open = true, listeners = NewServers}};
handle_info({'DOWN', _MonitorRef, process, Listener, _Info}, #state{listeners = Listeners} = State) ->
  NewListeners = sets:del_element(Listener, Listeners),
  {ok, State#state{listeners = NewListeners}};
handle_info(E, S) ->
  lager:warning("unexpected: ~p~n", [E]),
  {ok, S}.

shutdown(#state{fn_id = NodeId}) ->
  catch ranch:stop_listener(NodeId).


build(Item, #state{field = undefined, format = <<"json">>}) ->
  flowdata:to_json(Item);
build(Item, #state{field = undefined, format = <<"msgpack">>}) ->
  flowdata:to_s_msgpack(Item);
build(Item, S = #state{field = Path}) ->
  Data = flowdata:field(Item, Path),
  format(Data, S).

format(Data, #state{format = <<"msgpack">>}) ->
  msgpack:pack(Data, [{map_format, jiffy}]);
format(Data, #state{format = <<"json">>}) ->
  jiffy:encode(Data);
format(Data, #state{format = <<"raw">>}) when is_binary(Data) ->
  Data;
format(Data, #state{}) ->
  term_to_binary(Data).



p_type(<<"line">>) -> line;
p_type(Other) -> Other.
