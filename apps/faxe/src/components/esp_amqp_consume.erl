%%%-------------------------------------------------------------------
%%% @author heyoka
%%% @copyright (C) 2019, <COMPANY>
%%% @doc
%%% Consume data from an amqp-broker like rabbitmq.
%%% If safe is true -> use internal ondisc queue, otherwise just emit to downstream nodes
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
   root_exchange,
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
   debug_mode = false,
   include_topic = true,
   topic_key,
   as,
   safe_mode = false,
   confirm = true,
   dedup_queue :: memory_queue:memory_queue(),
   last_chan = undefined
}).

options() -> [
   {host, binary, {amqp, host}},
   {port, integer, {amqp, port}},
   {user, string, {amqp, user}},
   {pass, string, {amqp, pass}},
   {ssl, is_set, false},
   {vhost, string, <<"/">>},
   {routing_key, string, undefined},
   {bindings, string_list, undefined},
   {queue, string},
   {queue_prefix, string, {rabbitmq, queue_prefix}},
   {consumer_tag, string, undefined},
   {exchange, string},
   {root_exchange, string, {rabbitmq, root_exchange}},
   {exchange_prefix, string, {rabbitmq, exchange_prefix}},
   {prefetch, integer, 70},
   {ack_every, integer, 10},
   {ack_after, duration, <<"5s">>},
   {use_flow_ack, bool, false},
   {safe, boolean, false},
   {dt_field, string, <<"ts">>},
   {dt_format, string, ?TF_TS_MILLI},
   {include_topic, bool, true},
   {topic_as, string, <<"topic">>},
   {as, string, undefined},
   {confirm, boolean, true},
   {dedup_size, integer, 100}
].

check_options() ->
   [
      {one_of_params, [routing_key, bindings]}
   ].

metrics() ->
   [
      {?METRIC_BYTES_READ, meter, []}
   ].

init({GraphId, NodeId} = Idx, _Ins,
   #{ host := Host0, port := Port, user := _User, pass := _Pass, vhost := _VHost, queue := Q,
      exchange := Ex, prefetch := Prefetch, routing_key := RoutingKey0, bindings := Bindings0,
      dt_field := DTField, dt_format := DTFormat, ssl := _UseSSL, include_topic := IncludeTopic,
      topic_as := TopicKey, ack_every := AckEvery0, ack_after := AckTimeout0, as := As, consumer_tag := CTag0,
      queue_prefix := QPrefix, root_exchange := RExchange, exchange_prefix := XPrefix
      , use_flow_ack := FlowAck,
   safe := Safe, confirm := Confirm,
   dedup_size := DedupSize
   } = Opts0) ->

   process_flag(trap_exit, true),
   AckTimeout = faxe_time:duration_to_ms(AckTimeout0),
   CTag = case CTag0 of undefined -> <<"c_", GraphId/binary, "-", NodeId/binary>>; _ -> CTag0 end,
   Host = binary_to_list(Host0),
   lager:info("opts before: ~p",[Opts0]),
   Opts = Opts0#{
      host => Host, consumer_tag => CTag,
      exchange => faxe_util:prefix_binary(Ex, XPrefix),
      root_exchange => faxe_util:prefix_binary(RExchange, XPrefix),
      queue => faxe_util:prefix_binary(Q, QPrefix),
      routing_key => faxe_util:to_rkey(RoutingKey0),
      bindings => faxe_util:to_rkey(Bindings0)
   },
   lager:info("opts: ~p",[Opts]),
   State = #state{
      include_topic = IncludeTopic, topic_key = TopicKey, as = As, dedup_queue = memory_queue:new(DedupSize),
      opts = Opts, prefetch = Prefetch, ack_every = AckEvery0, ack_after = AckTimeout, flow_ack = FlowAck,
      dt_field = DTField, dt_format = DTFormat, safe_mode = Safe, flownodeid = Idx, confirm = Confirm},

   NewState = maybe_init_q(State),

   connection_registry:reg(Idx, Host, Port, <<"amqp">>),
   {ok, start_consumer(NewState)}.

maybe_init_q(State = #state{safe_mode = false}) ->
   State;
maybe_init_q(State = #state{flownodeid = Idx}) ->
   QFile = faxe_config:q_file(Idx),
   QConf = proplists:delete(ttf, faxe_config:get_esq_opts()),
   {ok, Q} = esq:new(QFile, QConf),
   start_emitter(State#state{queue = Q}).

process(_In, _, State = #state{}) ->
   {ok, State}.

%%
%% new queue-message arrives ...
%%
handle_info({ {DTag, RKey}, {Payload, CorrelationId, _Headers}, Channel},
    State=#state{flownodeid = FNId, dedup_queue = Dedup}) ->
   node_metrics:metric(?METRIC_BYTES_READ, byte_size(Payload), FNId),
   node_metrics:metric(?METRIC_ITEMS_IN, 1, FNId),

   NewState = State#state{last_chan = Channel},
   case CorrelationId /= undefined andalso memory_queue:member(CorrelationId, Dedup) of
      true ->
%%         lager:info("duplicate message found! [~p]",[CorrelationId]),
         {ok, maybe_ack(DTag, NewState)};
      false ->
         %% store correlation_id
         NewDedup = memory_queue:enq(CorrelationId, Dedup),
         Item0 = build_item(Payload, RKey, NewState),
         Item = flowdata:set_dtag(Item0, DTag),
         dataflow:maybe_debug(item_in, 1, Item, FNId, NewState#state.debug_mode),
         enq_or_emit(Item, DTag, NewState#state{dedup_queue = NewDedup})
   end;

handle_info({'DOWN', _MonitorRef, process, Consumer, _Info}, #state{consumer = Consumer} = State) ->
   connection_registry:disconnected(),
   lager:notice("MQ-Consumer ~p is 'DOWN'",[Consumer]),
   {ok, start_consumer(State)};
handle_info({'DOWN', _MonitorRef, process, Emitter, _Info}, #state{emitter = Emitter} = State) ->
   lager:notice("Q-Emitter ~p is 'DOWN'",[Emitter]),
   {ok, start_emitter(State)};
handle_info(ack_timeout, State = #state{last_dtag = undefined}) ->
   {ok, State#state{ack_timer = undefined}};
handle_info(ack_timeout, State = #state{collected = _Num}) ->
   NewState = do_ack(State),
   {ok, NewState};
handle_info(start_debug, State) -> {ok, State#state{debug_mode = true}};
handle_info(stop_debug, State) -> {ok, State#state{debug_mode = false}};
handle_info(Other, #state{consumer = Client} = State) when is_pid(Client) ->
   lager:notice("AmqpConsumer Info:~p, client: ~p", [Other, Client]),
   {ok, State};
handle_info(_R, State) ->
   {ok, State}.

handle_ack(_, _, State=#state{flow_ack = false}) ->
   {ok, State};
handle_ack(Mode, DTag, State=#state{consumer = Consumer}) ->
   lager:debug("got ack ~p for Tag: ~p",[Mode, DTag]),
   Func = case Mode of single -> ack; multi -> ack_multiple end,
   carrot:Func(Consumer, DTag),
   {ok, State}.

shutdown(#state{consumer = C, last_dtag = _DTag, emitter = Emitter}) ->
%%   case DTag of
%%      undefined -> ok;
%%      _ -> carrot:ack_multiple(C, DTag)
%%   end,
   catch gen_server:stop(C),
   catch (gen_server:stop(Emitter)).

enq_or_emit(Item, DTag, State = #state{safe_mode = false}) ->
   {emit, Item, maybe_ack(DTag, State)};
enq_or_emit(Item, DTag, State = #state{queue = Q}) ->
   ok = esq:enq(Item, Q),
   {ok, maybe_ack(DTag, State)}.

maybe_ack(_NewDTag, State = #state{confirm = false}) ->
   State;
maybe_ack(NewDTag, State = #state{collected = NumCollected}) ->
   maybe_ack(State#state{collected = NumCollected+1, last_dtag = NewDTag}).

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
%%   lager:info("ack_multiple to ~p",[DTag]),
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

build_item(Payload, RKey,
        #state{as = As, include_topic = AddTopic, topic_key = TopicKey, dt_field = DTField, dt_format = DTFormat}) ->
   Msg0 = flowdata:from_json_struct(Payload, DTField, DTFormat),
   Item0 =
      case AddTopic of
         true -> flowdata:set_field(Msg0, TopicKey, RKey);
         false -> Msg0
      end,
   flowdata:set_root(Item0, As).

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
consumer_config(Opts = #{vhost := VHost, queue := Q, consumer_tag := ConsumerTag,
   prefetch := Prefetch, exchange := XChange, root_exchange := RootEx, bindings := Bindings,
   routing_key := RoutingKey, confirm := Confirm}) ->

   % Number of connections not relevant here,
   % because we start the consumer monitored not pooled
   Config =
      [
         {workers, 1},
         {callback, self()},
         {confirm, Confirm},
         {setup_type, permanent},
         {consumer_tag, ConsumerTag},
         {prefetch_count, Prefetch},
         {vhost, VHost},
         {setup, [
               {queue, [
                  {queue, Q},
                  {exchange, XChange},
                  {routing_key, RoutingKey},
                  {bindings, Bindings}
               ]},
               {exchange, [
                         {exchange, XChange},
                         {type, <<"topic">>},
                         {source, RootEx}
                         ]}
         ]}
      ],
   Props = carrot_util:proplists_merge(
      maps:to_list(Opts) ++ [{ssl_opts, faxe_config:get_amqp_ssl_opts()}], Config),
%%   lager:warning("giving carrot these Configs: ~p", [Props]),
   Props.
