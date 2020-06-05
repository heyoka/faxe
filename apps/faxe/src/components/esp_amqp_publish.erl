%%%-------------------------------------------------------------------
%%% @author heyoka
%%% @copyright (C) 2019, <COMPANY>
%%% @doc
%%% Publish data to an amqp-broker like rabbitmq.
%%%
%%%
%%% port() is 5672 default
%%% ssl() is false by default
%%% @end
%%% Created : 10. October 2019 11:17
%%%-------------------------------------------------------------------
-module(esp_amqp_publish).
-author("heyoka").

%% API
-behavior(df_component).

-include("faxe.hrl").
%% API
-export([init/3, process/3, options/0, handle_info/2, shutdown/1, metrics/0]).

-define(DEFAULT_PORT, 5672).
-define(DEFAULT_SSL_PORT, 8883).

%% state for direct publish mode
-record(state, {
   client,
   connected = false,
   host,
   port,
   vhost,
   exchange,
   routing_key = false,
   ssl = false,
   opts,
   queue,
   flowid_nodeid
}).

options() -> [
   {host, binary, {amqp, host}},
   {port, integer, {amqp, port}},
   {user, string, {amqp, user}},
   {pass, string, {amqp, pass}},
   {vhost, binary, <<"/">>},
   {routing_key, binary},
   {exchange, binary},
   {ssl, bool, false}].

metrics() ->
   [
      {?METRIC_BYTES_SENT, meter, []}
   ].

init({GraphId, NodeId} = Idx, _Ins,
   #{ host := Host0, port := Port, user := _User, pass := _Pass, vhost := _VHost, exchange := Ex,
      routing_key := RoutingKey, ssl := _UseSSL} = Opts0) ->

   Host = binary_to_list(Host0),
   Opts = Opts0#{host => Host},
   EsqBaseDir = faxe_config:get(esq_base_dir),
   lager:info("esq_base_dir: ~p", [EsqBaseDir]),
   QFile = binary_to_list(<<EsqBaseDir/binary, GraphId/binary, "/", NodeId/binary>>),
   {ok, Q} = esq:new(QFile, [{tts, 300}, {capacity, 10}, {ttf, 20000}]),
   connection_registry:reg(Idx, Host, Port),
   connection_registry:connecting(),
   State = start_connection(#state{opts = Opts, exchange = Ex, routing_key = RoutingKey, queue = Q}),
   {ok, State#state{flowid_nodeid = Idx}}.

process(_In, Item, State = #state{exchange = Exchange, routing_key = Key, queue = Q, flowid_nodeid = FNId}) ->
   Payload = flowdata:to_json(Item),
   node_metrics:metric(?METRIC_BYTES_SENT, byte_size(Payload), FNId),
   node_metrics:metric(?METRIC_ITEMS_OUT, 1, FNId),
   ok = esq:enq({Exchange, Key, Payload, []}, Q),
   {ok, State}.

handle_info({publisher_ack, Ref}, State) ->
   lager:notice("message acked: ~p",[Ref]),
   {ok, State};
handle_info({'DOWN', _MonitorRef, process, Client, _Info}, #state{client = Client} = State) ->
   connection_registry:disconnected(),
   lager:notice("MQ-PubWorker ~p is 'DOWN'", [Client]),
   {ok, start_connection(State)}.

shutdown(#state{client = C, queue = Q}) ->
   catch (bunny_esq_worker:stop(C)),
   catch (exit(Q)).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% internal
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
start_connection(State = #state{opts = Opts, queue = Q}) ->
   {ok, Pid} = bunny_esq_worker:start(Q, build_config(Opts)),
   erlang:monitor(process, Pid),
   connection_registry:connected(),
   State#state{client = Pid}.


-spec build_config(Opts :: map()) -> list().
build_config(_Opts = #{vhost := VHost, host := Host, port := Port, user := User, pass := Pass}) ->
   HostParams = %% connection parameters
   [
      {hosts, [ {Host, Port} ]},
      {user, User},
      {pass, Pass},
      {reconnect_timeout, 2000},
      {ssl_options, none}, % Optional. Can be 'none' or [ssl_option()]
      {heartbeat, 60},
      {vhost, VHost}
   ],
   lager:info("giving bunny_esq_worker these Configs: ~p", [HostParams]),
   HostParams.


