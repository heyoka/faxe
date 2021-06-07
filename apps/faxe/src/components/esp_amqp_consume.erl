%%%-------------------------------------------------------------------
%%% @author heyoka
%%% @copyright (C) 2019, <COMPANY>
%%% @doc
%%% Consume data from an amqp-broker like rabbitmq.
%%%
%%%
%%% @end
%%% Created : 27. May 2019 09:00
%%%-------------------------------------------------------------------
-module(esp_amqp_consume).
-author("heyoka").

%% API
-behavior(df_component).

-include("faxe.hrl").
%% API
-export([
   init/3,
   process/3,
   options/0,
   handle_info/2,
   shutdown/1,
   metrics/0,
   check_options/0,
   handle_ack/3]).

-define(RECONNECT_TIMEOUT, 2000).

%% state for direct publish mode
-record(state, {
   consumer,
   host,
   port,
   user,
   pass,
   vhost,
   queue,
   exchange,
   routing_key = false,
   bindings = false,
   prefetch,
   collected = 0,
   ack_every,
   ack_after,
   ack_timer,
   flow_ack,
   last_dtag,
   ssl = false,
   opts,
   dt_field,
   dt_format,
   emitter,
   flownodeid,
   save = false,
   debug_mode = false,
   include_topic = true,
   topic_key,
   as
}).

options() -> [
   {host, binary, {amqp, host}},
   {port, integer, {amqp, port}},
   {user, string, {amqp, user}},
   {pass, string, {amqp, pass}},
   {ssl, is_set, false},
   {vhost, string, <<"/">>},
   {routing_key, string, undefined},
   {bindings, string_list, []},
   {queue, string},
   {exchange, string},
   {prefetch, integer, 10},
   {ack_every, integer, 5},
   {ack_after, duration, <<"5s">>},
   {use_flow_ack, bool, true},
   {dt_field, string, <<"ts">>},
   {dt_format, string, ?TF_TS_MILLI},
   {safe, is_set, false},
   {include_topic, bool, true},
   {topic_as, string, <<"topic">>},
   {as, string, undefined}
].

check_options() ->
   [
      {one_of_params, [routing_key, bindings]}
   ].

metrics() ->
   [
      {?METRIC_BYTES_READ, meter, []}
   ].

init({_GraphId, _NodeId} = Idx, _Ins,
   #{ host := Host0, port := Port, user := _User, pass := _Pass, vhost := _VHost, queue := _Q,
      exchange := _Ex, prefetch := Prefetch, routing_key := _RoutingKey, bindings := _Bindings,
      dt_field := DTField, dt_format := DTFormat, ssl := _UseSSL, include_topic := IncludeTopic,
      topic_as := TopicKey, ack_every := AckEvery0, ack_after := AckTimeout0, as := As,
      use_flow_ack := FlowAck} = Opts0) ->

   process_flag(trap_exit, true),
   AckTimeout = faxe_time:duration_to_ms(AckTimeout0),
   Host = binary_to_list(Host0),
   Opts = Opts0#{host => Host},
   State = #state{
      include_topic = IncludeTopic, topic_key = TopicKey, as = As,
      opts = Opts, prefetch = Prefetch, ack_every = AckEvery0, ack_after = AckTimeout,
      dt_field = DTField, dt_format = DTFormat, flow_ack = FlowAck},

   QFile = faxe_config:q_file(Idx),
   QConf = proplists:delete(ttf, faxe_config:get_esq_opts()),
   {ok, Q} = esq:new(QFile, QConf),

   NewState = start_emitter(State#state{queue = Q, flownodeid = Idx}),
   connection_registry:reg(Idx, Host, Port, <<"amqp">>),
   {ok, start_consumer(NewState)}.

process(_In, _, State = #state{}) ->
   {ok, State}.

%%
%% new queue-message arrives ...
%%
handle_info({ {DTag, RKey}, {Payload, _Headers}, _From},
    State=#state{queue = Q, flownodeid = FNId, collected = NumCollected}) ->
   node_metrics:metric(?METRIC_BYTES_READ, byte_size(Payload), FNId),
   node_metrics:metric(?METRIC_ITEMS_IN, 1, FNId),
   DataPoint0 = build_point(Payload, RKey, State),
   DataPoint = DataPoint0#data_point{dtag = DTag},
   ok = esq:enq(DataPoint, Q),
   dataflow:maybe_debug(item_in, 1, DataPoint, FNId, State#state.debug_mode),
   NewState = maybe_ack(State#state{collected = NumCollected+1, last_dtag = DTag}),
   {ok, NewState};

handle_info({'DOWN', _MonitorRef, process, Consumer, _Info}, #state{consumer = Consumer} = State) ->
   connection_registry:disconnected(),
   lager:notice("MQ-Consumer ~p is 'DOWN'",[Consumer]),
   {ok, start_consumer(State)};
handle_info({'DOWN', _MonitorRef, process, Emitter, _Info}, #state{emitter = Emitter} = State) ->
   lager:notice("Q-Emitter ~p is 'DOWN'",[Emitter]),
   {ok, start_emitter(State)};
handle_info(Other, #state{consumer = Client} = State) ->
   lager:warning("AmqpConsumer is 'DOWN' Info:~p, client: ~p", [Other, Client]),
   {ok, State};
handle_info(ack_timeout, State = #state{last_dtag = undefined}) ->
   {ok, State#state{ack_timer = undefined}};
handle_info(ack_timeout, State = #state{collected = _Num}) ->
   NewState = do_ack(State),
   {ok, NewState};
handle_info(start_debug, State) -> {ok, State#state{debug_mode = true}};
handle_info(stop_debug, State) -> {ok, State#state{debug_mode = false}};
handle_info(_R, State) ->
   {ok, State}.

handle_ack(Mode, DTag, State=#state{consumer = From}) ->
   lager:warning("got ack ~p for Tag: ~p",[Mode, DTag]),
   Func = case Mode of single -> ack; multi -> ack_multiple end,
   carrot:Func(From, DTag),
   {ok, State}.

shutdown(#state{consumer = C, last_dtag = DTag}) ->
   case DTag of
      undefined -> ok;
      _ -> carrot:ack_multiple(C, DTag)
   end,
   catch (rmq_consumer:stop(C)).

maybe_ack(State = #state{flow_ack = true}) ->
   State;
maybe_ack(State = #state{last_dtag = undefined, collected = 0}) ->
   State;
maybe_ack(State = #state{collected = NumCollected, ack_every = NumCollected}) ->
   do_ack(State);
maybe_ack(State = #state{collected = 1}) ->
   restart_ack_timeout(State);
maybe_ack(State) ->
   State.

do_ack(State = #state{last_dtag = DTag, consumer = From, ack_timer = Timer, collected = _Num}) ->
   catch erlang:cancel_timer(Timer),
   carrot:ack_multiple(From, DTag),
   State#state{collected = 0, last_dtag = undefined, ack_timer = undefined}.

restart_ack_timeout(State = #state{ack_after = Time, ack_timer = Timer}) ->
   catch erlang:cancel_timer(Timer),
   NewTimer = erlang:send_after(Time, self(), ack_timeout),
   State#state{ack_timer = NewTimer}.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% internal
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
build_point(Payload, RKey,
    #state{as = As, include_topic = AddTopic, topic_key = TopicKey, dt_field = DTField, dt_format = DTFormat}) ->
   Msg0 = flowdata:from_json_struct(Payload, DTField, DTFormat),
   P0 =
   case AddTopic of
      true -> flowdata:set_field(Msg0, TopicKey, RKey);
      false -> Msg0
   end,
   case As of
      undefined -> P0;
      Root -> #data_point{fields = Fields} = P0, P0#data_point{fields = #{Root => Fields}}
   end.

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
%%   lager:warning("giving carrot these Configs: ~p", [Props]),
   Props.


