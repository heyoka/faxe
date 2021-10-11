%%%-------------------------------------------------------------------
%%% @author heyoka
%%% @copyright (C) 2021
%%% @doc
%%% The mqtt_amqp-bridge node provides a message-order preserving and fail-safe mqtt-to-amqp bridge.
%%% It is designed for minimized overhead, high throughput and fault-tolerant message delivery.
%%% Receives data from an mqtt-broker and writes each indiviual topic with it's own amqp-publisher.
%%% This node starts 1 mqtt-subscriber and for every unique topic it sees, it will start an amqp-publisher.
%%%
%%% The node does only work standalone at the moment, meaning you can not connect it to other nodes.
%%% It is completely unaware of the message content.
%%% For performance reasons the node does not use data_items as every other node
%%% in faxe does, instead internally it will work with the raw binaries received from the mqtt broker.
%%%
%%% @end
%%% Created : 10. Feb 2021 09:00
%%%-------------------------------------------------------------------
-module(esp_mqtt_amqp_bridge).
-author("heyoka").

%% API
-behavior(df_component).

-include("faxe.hrl").
%% API
-export([init/3, process/3, options/0, handle_info/2, shutdown/1, check_options/0, metrics/0, data_received/3]).

%% state for direct publish mode
-record(state, {
   client,
   connected = false,
   reconnector,
   host,
   port,
   user,
   pass,
   qos,
   topic,
   topics,
   client_id,
   ssl = false,
   ssl_opts = [],
   fn_id,

   %% AMQP
   amqp_opts,
   amqp_exchange,

   %% other
   max_publishers = 5,
   topic_to_queue = #{},
   queue_to_topics = #{},
   publisher_to_queue = #{},

   topic_last_seen = #{},
   reset_timeout,
   reset_check_interval = 30000,

   safe_mode = false,
   queue_opts
   }).

options() -> [
   %% MQTT
   {host, binary, {mqtt, host}},
   {port, integer, {mqtt, port}},
   {user, string, {mqtt, user}},
   {pass, string, {mqtt, pass}},
   {qos, integer, 1},
   {topic, binary, undefined},
   {topics, binary_list, undefined},
   {ssl, is_set},
   %% AMQP
   {amqp_host, string, {amqp, host}},
   {amqp_port, integer, {amqp, port}},
   {amqp_user, string, {amqp, user}},
   {amqp_pass, string, {amqp, pass}},
   {amqp_vhost, string, <<"/">>},
   {amqp_exchange, string, <<"x">>},
   {amqp_ssl, is_set, false},
   {persistent, bool, false},
   %% OTHER
   {reset_timeout, duration, <<"5m">>},
   {max_publishers, integer, 3},
   {safe, is_set, false}

].

check_options() ->
   [
      {one_of_params, [topic, topics]}
   ].

metrics() ->
   [
      {?METRIC_BYTES_READ, meter, [], "Size of item read in kib."},
      {?METRIC_BYTES_SENT, meter, [], "Size of item sent in kib."}
   ].

init(NodeId, _Ins,
      %% MQTT
   #{ host := Host0, port := Port, topic := Topic, topics := Topics, user := User, pass := Pass,
      ssl := UseSSL, qos := Qos,
      %% AMQP
      amqp_host := AMQPHost0, amqp_port := AMQPPort, amqp_user := AMQPUser, amqp_pass := AMQPPass,
      amqp_vhost := AMQPVHost, amqp_exchange := AMQPEx, amqp_ssl := AMQPUseSSL, persistent := Persistent,
      %% OTHER
      reset_timeout := RTimeout, max_publishers := MaxPublishers, safe := Safe
      } = _Opts) ->

   %% AMQP
   AMQPOpts = #{
      host => binary_to_list(AMQPHost0), port => AMQPPort, user => AMQPUser,
      pass => AMQPPass, vhost => AMQPVHost, exchange => AMQPEx,
      ssl => AMQPUseSSL, safe_mode => Safe, persistent => Persistent
   },

   %% MQTT
   Host = binary_to_list(Host0),
   process_flag(trap_exit, true),
   ClientId = list_to_binary(faxe_util:uuid_string()),

   %% queue
   QOpts0 = faxe_config:get_esq_opts(),
   QOpts = case Safe of true -> QOpts0; false -> proplists:delete(ttf, QOpts0) end,

   reconnect_watcher:new(10000, 5, io_lib:format("~s:~p ~p",[Host, Port, ?MODULE])),
   Reconnector = faxe_backoff:new({30, 3200}),
   {ok, Reconnector1} = faxe_backoff:execute(Reconnector, connect_mqtt),

   connection_registry:reg(NodeId, Host, Port, <<"mqtt">>),

   ResetTimeout = faxe_time:duration_to_ms(RTimeout),

   State = #state{
      reset_timeout = ResetTimeout, max_publishers = MaxPublishers, safe_mode = Safe, queue_opts = QOpts,
      amqp_opts = AMQPOpts, amqp_exchange = AMQPEx,

      host = Host, port = Port, topic = Topic, ssl = UseSSL, qos = Qos,
      client_id = ClientId, topics = Topics, reconnector = Reconnector1, user = User,
      pass = Pass, fn_id = NodeId, ssl_opts = ssl_opts(UseSSL)},

   %% start reset interval
   erlang:send_after(State#state.reset_check_interval, self(), check_reset),

   {ok, State}.

ssl_opts(false) ->
   [];
ssl_opts(true) ->
   faxe_config:get_mqtt_ssl_opts().

process(_In, _, State = #state{}) ->
   {ok, State}.


handle_info(connect_mqtt, State) ->
   connect_mqtt(State),
   {ok, State};
handle_info({mqttc, C, connected}, State=#state{host = Host, reconnector = Recon}) ->
   connection_registry:connected(),
   lager:notice("mqtt client connected to: ~p !!", [Host]),
   NewState = State#state{client = C, connected = true, reconnector = faxe_backoff:reset(Recon)},
   subscribe(NewState),
   {ok, NewState};
handle_info({mqttc, _C,  disconnected}, State=#state{client = Client}) ->
   catch exit(Client, kill),
   connection_registry:disconnected(),
   lager:debug("mqtt client disconnected!!"),
   {ok, State#state{connected = false, client = undefined}};
%% for emqtt
handle_info({publish, #{payload := Payload, topic := Topic} }, S=#state{}) ->
   data_received(Topic, Payload, S);
%% for emqttc
handle_info({publish, Topic, Payload }, S=#state{}) ->
   data_received(Topic, Payload, S);
handle_info({disconnected, shutdown, tcp_closed}=M, State = #state{}) ->
   lager:warning("emqtt : ~p", [M]),
   {ok, State};
handle_info({'EXIT', C, _Reason}, State = #state{reconnector = Recon, host = H, port = P, client = C}) ->
   connection_registry:disconnected(),
   lager:warning("EXIT emqtt: ~p [~p]", [_Reason,{H, P}]),
   {ok, Reconnector} = faxe_backoff:execute(Recon, connect_mqtt),
   {ok, State#state{connected = false, client = undefined, reconnector = Reconnector}};
%% esq process is down
handle_info({'EXIT', Q, _Reason}, State = #state{publisher_to_queue = Pubs}) ->
   Flipped = faxe_util:flip_map(Pubs),
   case is_map_key(Q, Flipped) of
      true -> handle_queue_exit(Q, State);
      false -> lager:warning("process ~p exited with Reason: ~p", [Q, _Reason]),
         {ok, State}
   end;
handle_info({'DOWN', _MonitorRef, process, Client, _Info}, #state{publisher_to_queue = Pubs} = State)
      when is_map_key(Client, Pubs) ->
   Q = maps:get(Client, Pubs),
   NewState = start_amqp_connection(Q, State#state{publisher_to_queue = maps:without([Client], Pubs)}),
   lager:notice("AMQP-PubWorker ~p is 'DOWN' Info:~p", [Client, _Info]),
   {ok, NewState};
handle_info({publisher_ack, Ref}, State) ->
   lager:notice("message acked: ~p",[Ref]),
   {ok, State};
handle_info(check_reset, State=#state{reset_check_interval = Interval}) ->
   lager:info("check_reset"),
   NewState = check_reset(State),
   erlang:send_after(Interval, self(), check_reset),
   {ok, NewState};
handle_info(What, State) ->
   lager:warning("~p handle_info: ~p", [?MODULE, What]),
   {ok, State}.

%% @todo shutdown esq queues ?
shutdown(#state{client = C, publisher_to_queue = PubsToQ}) ->
   catch (emqttc:disconnect(C)),
   [catch begin bunny_esq_worker:stop(CPub), esq:free(Q) end || {CPub, Q} <- maps:to_list(PubsToQ)]
.

handle_queue_exit(Q, State = #state{queue_to_topics = QueueTopics,
      topic_to_queue = TopicQueue, publisher_to_queue = Pubs}) ->
   %% stop the publisher, we need a new one anyway
   Publisher = maps:get(Q, faxe_util:flip_map(Pubs)),
   lager:warning("will stop amqp_publisher, because esq process died: ~p",[Publisher]),
   gen_server:stop(Publisher),

   %% delete all map entries from topic_to_queue, where the Q is the exited one
   NewQTopics = maps:without([Q], QueueTopics),
   NewTopicQs = maps:fold(
      fun(K, V, Acc) ->
         case V of
            Q -> Acc;
            _ -> Acc#{K => V}
         end
      end, #{}, TopicQueue),

%%   lager:notice("Q ~p EXIT before cleanup: Q-to-Topics ~p, TopicToQueue: ~p",[Q, QueueTopics, TopicQueue]),
%%   lager:notice("Q ~p EXIT AFTER cleanup: Q-to-Topics ~p, TopicToQueue: ~p",[Q, NewQTopics, NewTopicQs]),
   {ok, State#state{queue_to_topics = NewQTopics, topic_to_queue = NewTopicQs,
      publisher_to_queue = maps:without([Publisher], Pubs)}}.

data_received(Topic, Payload, S = #state{topic_to_queue = Queues, amqp_exchange = Exchange, topic_last_seen = Last,
   fn_id = FNId})
      when is_map_key(Topic, Queues) ->
   Q = maps:get(Topic, Queues),
   RoutingKey = topic_to_key(Topic),
%%   lager:notice("got data with topic: ~p and enqueue with routingkey :~p",[Topic, RK]),
   ok = esq:enq({Exchange, RoutingKey, Payload, []}, Q),
   ByteSize = byte_size(Payload),
   node_metrics:metric(?METRIC_BYTES_SENT, ByteSize, FNId),
   node_metrics:metric(?METRIC_BYTES_READ, ByteSize, FNId),
   node_metrics:metric(?METRIC_ITEMS_IN, 1, FNId),
   node_metrics:metric(?METRIC_ITEMS_OUT, 1, FNId),
   {ok, S#state{topic_last_seen = Last#{Topic => faxe_time:now()}}};
data_received(Topic, Payload, S = #state{}) ->
   lager:warning("get new queue and publisher: ~p",[Topic]),
   NewState = new_queue_publisher(Topic, S),
   data_received(Topic, Payload, NewState).

new_queue_publisher(Topic, State = #state{max_publishers = MaxPubs,
   queue_to_topics = QueueTopics}) when map_size(QueueTopics) < MaxPubs ->
   lager:warning("start new queue and publisher: ~p", [Topic]),
   NewState0 = start_queue(Topic, State),
   start_amqp_connection(Topic, NewState0);
new_queue_publisher(Topic, State = #state{queue_to_topics = QueueTopics, topic_to_queue = TopicQueue}) ->
   lager:warning("add topic to existing queue: ~p",[Topic]),
   QTopics = maps:to_list(QueueTopics),
   SortFun = fun({_AQ, ATopics}, {_BQ, BTopics}) -> length(ATopics) =< length(BTopics) end,
   [{Q, Topics} | _] = lists:sort(SortFun, QTopics),
   lager:notice("add: ~p to q: ~p, (~p)",[Topic, Q, Topics]),
   QueueToTopicsNew = QueueTopics#{Q => [Topic|Topics]},
   State#state{queue_to_topics = QueueToTopicsNew, topic_to_queue = TopicQueue#{Topic => Q}}.

start_amqp_connection(Q, State = #state{amqp_opts = Opts, publisher_to_queue = PubQ})
      when is_pid(Q) ->
   {ok, Pid} = bunny_esq_worker:start(Q, Opts),
   erlang:monitor(process, Pid),
   State#state{publisher_to_queue = PubQ#{Pid => Q}};
start_amqp_connection(Topic, State = #state{topic_to_queue = Queues}) ->
   Q = maps:get(Topic, Queues),
   start_amqp_connection(Q, State).

start_queue(Topic, State = #state{fn_id = {FlowId, NodeId},
      topic_to_queue = TopicQueue, queue_to_topics = QTopics, queue_opts = QOpts}) ->
   Key = topic_to_key(Topic),
   QFile = faxe_config:q_file({FlowId, <<NodeId/binary, "-", Key/binary>>}),
   {ok, Q} = esq:new(QFile, QOpts),
   NewQueues = TopicQueue#{Topic => Q},
   State#state{topic_to_queue = NewQueues, queue_to_topics = QTopics#{Q => [Topic]}}.

connect_mqtt(State = #state{host = Host, port = Port, client_id = ClientId}) ->
   connection_registry:connecting(),
   reconnect_watcher:bump(),
   Opts0 = [
      {host, Host},
      {port, Port},
      {keepalive, 25},
      {reconnect, 3, 120, 10},
      {client_id, ClientId}
   ],
   Opts1 = opts_auth(State, Opts0),
   Opts = opts_ssl(State, Opts1),
   lager:info("connect to mqtt broker with: ~p",[Opts]),
   {ok, _Client} = emqttc:start_link(Opts)
.

topic_to_key(Topic) when is_binary(Topic) ->
   binary:replace(Topic, <<"/">>, <<".">>, [global]).

opts_auth(#state{user = <<>>}, Opts) -> Opts;
opts_auth(#state{user = undefined}, Opts) -> Opts;
opts_auth(#state{user = User, pass = Pass}, Opts) ->
   [{username, User},{password, Pass}] ++ Opts.
opts_ssl(#state{ssl = false}, Opts) -> Opts;
opts_ssl(#state{ssl = true, ssl_opts = SslOpts}, Opts) ->
   [{ssl, SslOpts}]++ Opts.


subscribe(#state{qos = Qos, client = C, topic = Topic, topics = undefined}) when is_binary(Topic) ->
   lager:info("mqtt_client subscribe: ~p", [Topic]),
   ok = emqttc:subscribe(C, Topic, Qos);
subscribe(#state{qos = Qos, client = C, topics = Topics}) ->
   TQs = [{Top, Qos} || Top <- Topics],
   ok = emqttc:subscribe(C, TQs).

check_reset(State = #state{topic_last_seen = LastSeen}) when map_size(LastSeen) == 0 ->
   State;
check_reset(State = #state{topic_last_seen = LastSeen, reset_timeout = Timeout}) ->
   Now = faxe_time:now(),
   Fun =
   fun(Topic, TopicLastSeen, Acc) ->
      case (TopicLastSeen + Timeout) < Now of
         true -> [Topic|Acc];
         false -> Acc
      end
   end,
   ResetList = maps:fold(Fun, [], LastSeen),
%%   lager:notice("reset list: ~p", [ResetList]),
   lists:foldl(fun(T, AccState) -> reset_topic(T, AccState) end, State, ResetList).

reset_topic(Topic,
      State = #state{topic_to_queue = TopicToQ, queue_to_topics = QToTopics, publisher_to_queue = Pubs}) ->

   NewTopicToQ = maps:without([Topic], TopicToQ),
   {NewQToTopics, NewPubs} =
      maps:fold(
      fun(Q, Topics, {QToTopicsAcc, PubsAcc}) ->
         NewTopics =
         case lists:member(Topic, Topics) of
            true -> lists:delete(Topic, Topics);
            false -> Topics
         end,
         case NewTopics of
            [] ->
               %% stop publisher and q and delete QTopics entry and publisher entry
               esq:free(Q),
               Publisher = maps:get(Q, faxe_util:flip_map(Pubs)),
               bunny_esq_worker:stop(Publisher),
               {QToTopicsAcc, maps:without([Publisher], PubsAcc)};
            _ -> {QToTopicsAcc#{Q => NewTopics}, PubsAcc}
         end
      end, {#{}, Pubs}, QToTopics),
   State#state{topic_to_queue = NewTopicToQ, queue_to_topics = NewQToTopics,
      publisher_to_queue = NewPubs, topic_last_seen = maps:without([Topic], State#state.topic_last_seen)}.


