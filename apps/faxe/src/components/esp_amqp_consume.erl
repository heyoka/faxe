%%%-------------------------------------------------------------------
%%% @author heyoka
%%% @copyright (C) 2019, <COMPANY>
%%% @doc
%%% Consume data from an amqp-broker like rabbitmq.
%%% When prefetch is given and is > 1, then this node will emit a data_batch record instead of a data_point
%%%
%%% port() is 5672 default
%%% ssl() is false by default
%%% @end
%%% Created : 27. May 2019 09:00
%%%-------------------------------------------------------------------
-module(esp_amqp_consume).
-author("heyoka").

%% API
-behavior(df_component).

-include("faxe.hrl").
%% API
-export([init/3, process/3, options/0, handle_info/2, shutdown/1, metrics/0]).

-define(RECONNECT_TIMEOUT, 2000).
-define(Q_OPTS, [{tts, 300}, {capacity, 10}]).

%% state for direct publish mode
-record(state, {
   consumer,
   connected = false,
   host,
   port,
   user,
   pass,
   vhost,
   queue,
   exchange,
   routing_key = false,
   routing_keys = false,
   prefetch,
   ssl = false,
   opts,
   dt_field,
   dt_format,
   collected = 0,
   points = queue:new(),
   emitter,
   flownodeid
}).

options() -> [
   {host, binary, {amqp, host}},
   {port, integer, {amqp, port}},
   {user, string, {amqp, user}},
   {pass, string, {amqp, pass}},
   {ssl, is_set, false},
   {vhost, binary, <<"/">>},
   {routing_key, binary, undefined},
   {routing_keys, string_list, []},
   {queue, binary},
   {exchange, binary},
   {prefetch, integer, 1},
   {dt_field, string, <<"ts">>},
   {dt_format, string, ?TF_TS_MILLI}
].

metrics() ->
   [
      {?METRIC_BYTES_READ, meter, []}
   ].

init({_GraphId, _NodeId} = Idx, _Ins,
   #{ host := Host0, port := Port, user := _User, pass := _Pass, vhost := _VHost, queue := _Q,
      exchange := _Ex, prefetch := Prefetch, routing_key := _RoutingKey, routing_keys := RKeys, dt_field := DTField,
      dt_format := DTFormat, ssl := _UseSSL}
      = Opts0) ->

   Host = binary_to_list(Host0),
   Opts1 = Opts0#{host => Host, bindings => RKeys},
   %% get rid of the routing_keys entry, it is named bindings
   Opts = maps:without([routing_keys], Opts1),
   State = #state{opts = Opts, prefetch = Prefetch, dt_field = DTField, dt_format = DTFormat},

   QFile = faxe_config:q_file(Idx),
   {ok, Q} = esq:new(QFile, ?Q_OPTS),

   NewState = start_emitter(State#state{queue = Q, flownodeid = Idx}),
   connection_registry:reg(Idx, Host, Port, <<"amqp">>),
   {ok, start_consumer(NewState)}.

process(_In, _, State = #state{}) ->
   {ok, State}.

%%
%% new queue-message arrives ...
%%
handle_info({ {DTag, RKey}, {Payload, _Headers}, From}, State=#state{prefetch = 1, queue = Q, flownodeid = FNId}) ->
   node_metrics:metric(?METRIC_BYTES_READ, byte_size(Payload), FNId),
   node_metrics:metric(?METRIC_ITEMS_IN, 1, FNId),
   DataPoint = build_point(Payload, RKey, State#state.dt_field, State#state.dt_format),
   ok = esq:enq(DataPoint, Q),
   carrot:ack(From, DTag),
   {ok, State};

handle_info({ {DTag, RKey}, {Payload, _Headers}, From},
    State=#state{collected = NumCollected, points = Batch}) ->
   DataPoint = build_point(Payload, RKey, State#state.dt_field, State#state.dt_format),
   NewState = State#state{points = queue:in(DataPoint, Batch), collected = NumCollected+1},
   maybe_emit(DTag, From, NewState);

handle_info({'DOWN', _MonitorRef, process, Consumer, _Info}, #state{consumer = Consumer} = State) ->
   connection_registry:disconnected(),
   lager:notice("MQ-Consumer ~p is 'DOWN'",[Consumer]),
   {ok, start_consumer(State)};
handle_info({'DOWN', _MonitorRef, process, Emitter, _Info}, #state{emitter = Emitter} = State) ->
   lager:notice("Q-Emitter ~p is 'DOWN'",[Emitter]),
   {ok, start_emitter(State)};
handle_info(_R, State) ->
   {ok, State}.

shutdown(#state{consumer = C}) ->
   catch (rmq_consumer:stop(C)).

maybe_emit(DTag, From, State = #state{prefetch = Prefetch, collected = Prefetch, points = Points}) ->
   DataBatch = #data_batch{points = queue:to_list(Points)},
   NewState = State#state{points = queue:new(), collected = 0},
%%   lager:notice("emit: ~p",[Prefetch]),
   carrot:ack_multiple(From, DTag),
%%   gen_event:notify(faxe_debug, {Key, {GId, NId}, Port, Value}).
   {emit, {1, DataBatch}, NewState};
maybe_emit(_DTag, _From, State = #state{}) ->
   {ok, State}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% internal
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
build_point(Payload, RKey, DTField, DTFormat) ->
   Msg0 = flowdata:from_json_struct(Payload, DTField, DTFormat),
   flowdata:set_field(Msg0, <<"topic">>, RKey).

start_consumer(State = #state{opts = ConsumerOpts}) ->
   connection_registry:connecting(),
   {ok, Pid, _NewConsumer} =
      rmq_consumer:start_monitor(self(), consumer_config(ConsumerOpts)),
   connection_registry:connected(),
   State#state{consumer = Pid}.

start_emitter(State = #state{queue = Q}) ->
   {ok, Emitter} = q_msg_forwarder:start_monitor(Q),
   State#state{emitter = Emitter}.


-spec consumer_config(Opts :: map()) -> list().
consumer_config(Opts = #{vhost := VHost, queue := Q,
   prefetch := Prefetch, exchange := XChange, bindings := Bindings, routing_key := RoutingKey}) ->
   lager:info("AMQP consumer_config: ~p",[Opts]),
%%   [
%%      {hosts, [ {Host, Port} ]},
%%      {user, User},
%%      {pass, Pass},
%%      {reconnect_timeout, ?RECONNECT_TIMEOUT},
%%      {ssl_options, none} % Optional. Can be 'none' or [ssl_option()]
%%   ],
   RMQConfig = faxe_config:get(rabbitmq),
   RootExchange = proplists:get_value(root_exchange, RMQConfig, <<"amq.topic">>),
%%   HostParams = %% connection parameters
   Config =
      [
         {workers, 1},  % Number of connections, but not relevant here,
         % because we start the consumer monitored
         {callback, self()},
         {setup_type, permanent},
         {prefetch_count, Prefetch},
         {vhost, VHost},
         {setup,
            [
               {queue, [
                  {queue, Q},
                  {exchange, XChange},
                  {routing_key, RoutingKey},
                  {bindings, Bindings}

               ]},
               {exchange, [
                         {exchange, XChange},
                         {type, <<"topic">>},
                         {source, faxe_util:to_bin(RootExchange)}
                         ]
                      }
            ]
         }

      ],
   Props = carrot_util:proplists_merge(maps:to_list(Opts) ++ [{ssl_opts, faxe_config:get_amqp_ssl_opts()}], Config),
   lager:warning("giving carrot these Configs: ~p", [Props]),
   Props.


