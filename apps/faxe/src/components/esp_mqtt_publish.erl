%%%-------------------------------------------------------------------
%%% @author heyoka
%%% @copyright (C) 2019, <COMPANY>
%%% @doc
%%% Publish every single message to a mqtt-broker.
%%% Incoming data_points or data_batches are converted to a JSON string before sending.
%%% If the safe() parameter is given, every message first gets stored in an on-disk queue before it will
%%% be sent, this way we can make sure no message gets lost when disconnected from the broker.
%%% Note that in safe mode, messages are not sent right away, so a delay of up to a second may be introduced.
%%% other params:
%%% port() is 1883 by default
%%% qos() is 1 by default
%%% ssl() is false by default
%%% retained() is false by default
%%% @end
%%% Created : 27. May 2019 09:00
%%%-------------------------------------------------------------------
-module(esp_mqtt_publish).
-author("heyoka").

%% API
-behavior(df_component).

-include("faxe.hrl").
%% API
-export([init/3, process/3, options/0, handle_info/2, shutdown/1, metrics/0, check_options/0]).

%% state for safe-mode
-record(state, {
   publisher,
   options,
   queue_file,
   queue,
   topic,
   topic_lambda,
   topic_field,
   safe = false,
   fn_id,
   debug_mode = false,
   use_pool = false :: true|false,
   pool_connected = false :: true|false
}).
%% state for direct publish mode

options() -> [
   {host, binary, {mqtt, host}},
   {port, integer, {mqtt, port}},
   {user, string, {mqtt, user}},
   {pass, string, {mqtt, pass}},
   {client_id, string, undefined},
   {qos, integer, 1},
   {topic, binary, undefined},
   {topic_field, binary, undefined},
   {topic_lambda, lambda, undefined},
   {retained, is_set},
   {ssl, is_set, {mqtt, ssl, enable}},
   {safe, boolean, false},
   {max_mem_queue_size, integer, 100},
   {use_pool, boolean, {mqtt_pub_pool, enable}}
].

check_options() ->
   [
      {one_of_params, [topic, topic_lambda, topic_field]}
   ].

metrics() ->
   [
%%      {?METRIC_SENDING_TIME, histogram, [slide, 60], "Network time for sending a message."},
      {?METRIC_BYTES_SENT, meter, [], "Size of item sent in kib."}
   ].

%% safe mode with ondisc queuing
init({_GraphId, _NodeId} = GId, _Ins, #{safe := true}=Opts) ->
   QFile = faxe_config:q_file(GId),
   QConf = proplists:delete(ttf, faxe_config:get_esq_opts()),
   {ok, Q} = esq:new(QFile, QConf),
   NewOpts = prepare_opts(GId, Opts),
   {ok, Publisher} = mqtt_publisher:start_link(NewOpts, Q),
   init_all(NewOpts, #state{publisher = Publisher, queue = Q, fn_id = GId});
%% direct publish mode
init(NodeId, _Ins, #{use_pool := true} = Opts) ->
   NewOpts = prepare_opts(NodeId, Opts),
%%   lager:info("use mqtt_pub_pool with opts: ~p",[NewOpts]),
   mqtt_pub_pool_manager:connect(NewOpts),
   init_all(NewOpts, #state{fn_id = NodeId});
init(NodeId, _Ins, #{safe := false} = Opts) ->
   NewOpts = prepare_opts(NodeId, Opts),
   {ok, Publisher} = mqtt_publisher:start_link(NewOpts),
   init_all(NewOpts, #state{publisher = Publisher, fn_id = NodeId}).

init_all(
    #{safe := Safe, topic := Topic, topic_lambda := LTopic, topic_field := TField,
       use_pool := Pool, host := Host, port := Port} = Opts, State = #state{fn_id = NId}) ->
   %% when using the connection pool, we have to take care of the connection_registry ourselves
   case Pool of
      true -> connection_registry:reg(NId, Host, Port, <<"mqtt">>);
      false -> ok
   end,
   {ok, all,
      State#state{
         options = Opts, safe = Safe, topic = Topic, topic_lambda = LTopic, topic_field = TField, use_pool = Pool}
   }.

prepare_opts({GId, NId}=GNId, Opts0 = #{client_id := CId, host := Host0}) ->
   Host = binary_to_list(Host0),
   ClientId = case CId of undefined -> <<GId/binary, "_", NId/binary>>; _ -> CId end,
   Opts0#{host => Host, client_id => ClientId, node_id => GNId}.

%% safe state
process(_In, Item, State = #state{safe = true, queue = Q, fn_id = FNId}) ->
   ok = esq:enq(build_message(Item, State), Q),
   dataflow:maybe_debug(item_out, 1, Item, FNId, State#state.debug_mode),
   {ok, State};
%% using the connection pool
process(_Inport, _Item, State = #state{use_pool = true, pool_connected = false}) ->
%%   lager:notice("got item, but pool not up yet"),
   {ok, State};
process(_Inport, Item, State = #state{safe = false, use_pool = true, fn_id = FNId,
      options = #{host := Host, qos := Qos, retained := Ret}}) ->
%%   {T, {ok, Publisher}} = timer:tc(mqtt_pub_pool_manager, get_connection, [Host]),
   {ok, Publisher} = mqtt_pub_pool_manager:get_connection(Host),
%%   lager:info("time to get connection ~p ~p",[Publisher, T]),
   {Topic, Message} = build_message(Item, State),
   Publisher ! {publish, {Topic, Message, Qos, Ret}},
   dataflow:maybe_debug(item_out, 1, Item, FNId, State#state.debug_mode),
   {ok, State};
process(_Inport, Item, State = #state{safe = false, publisher = Publisher, fn_id = FNId}) ->
%%   lager:warning("send msg when not safe and no pool used: ~p",[lager:pr(State, ?MODULE)]),
   Publisher ! {publish, build_message(Item, State)},
   dataflow:maybe_debug(item_out, 1, Item, FNId, State#state.debug_mode),
   {ok, State}.

%% we only get these, when pool is used
handle_info({mqtt_connected, _}, State) ->
%%   lager:info("mqtt_pool connected"),
   connection_registry:connected(),
   {ok, State#state{pool_connected = true}};
handle_info({mqtt_disconnected, _}, State) ->
   connection_registry:disconnected(),
   {ok, State#state{pool_connected = false}};

handle_info(start_debug, State) -> {ok, State#state{debug_mode = true}};
handle_info(stop_debug, State) -> {ok, State#state{debug_mode = false}};
handle_info(_E, S) ->
   {ok, S}.

shutdown(#state{publisher = P}) ->
   catch gen_server:stop(P).

build_message(Item, State = #state{fn_id = FNId}) ->
   Json = flowdata:to_json(Item),
   node_metrics:metric(?METRIC_BYTES_SENT, byte_size(Json), FNId),
   node_metrics:metric(?METRIC_ITEMS_OUT, 1, FNId),
   {get_topic(Item, State), Json}.


get_topic(_Item, # state{topic_lambda = undefined, topic_field = undefined, topic = Topic}) ->
   Topic;
get_topic(Item = #data_point{}, # state{topic_lambda = undefined, topic = undefined, topic_field = TField}) ->
   flowdata:field(Item, TField);
get_topic(#data_batch{points = [P|_]}, # state{topic_lambda = undefined, topic = undefined, topic_field = TField}) ->
   flowdata:field(P, TField);
get_topic(#data_point{} = P, #state{topic_lambda = Fun}) ->
   faxe_lambda:execute(P, Fun);
get_topic(#data_batch{points = [P1|_]}, #state{topic_lambda = Fun}) ->
   faxe_lambda:execute(P1, Fun).


