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
   debug_mode = false
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
   {max_mem_queue_size, integer, 100}
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
init({_GraphId, _NodeId} = GId, _Ins, #{safe := true, host := Host0}=Opts) ->
   QFile = faxe_config:q_file(GId),
   QConf = proplists:delete(ttf, faxe_config:get_esq_opts()),
   {ok, Q} = esq:new(QFile, QConf),
   NewOpts = prepare_opts(GId, Opts),
   {ok, Publisher} = mqtt_publisher:start_link(NewOpts, Q),
   init_all(NewOpts, #state{publisher = Publisher, queue = Q, fn_id = GId});
%% direct publish mode
init(NodeId, _Ins, #{safe := false} = Opts) ->
   NewOpts = prepare_opts(NodeId, Opts),
   {ok, Publisher} = mqtt_publisher:start_link(NewOpts),
   init_all(NewOpts, #state{publisher = Publisher, fn_id = NodeId}).

init_all(#{safe := Safe, topic := Topic, topic_lambda := LTopic, topic_field := TField} = Opts, State) ->
   {ok, all, State#state{options = Opts, safe = Safe, topic = Topic, topic_lambda = LTopic, topic_field = TField}}.

prepare_opts({GId, NId}=GNId, Opts0 = #{client_id := CId, host := Host0}) ->
   Host = binary_to_list(Host0),
   ClientId = case CId of undefined -> <<GId/binary, "_", NId/binary>>; _ -> CId end,
   Opts0#{host => Host, client_id => ClientId, node_id => GNId}.

%% safe state
process(_In, Item, State = #state{safe = true, queue = Q, fn_id = FNId}) ->
   ok = esq:enq(build_message(Item, State), Q),
   dataflow:maybe_debug(item_out, 1, Item, FNId, State#state.debug_mode),
   {ok, State};
%% direct state
process(_Inport, Item, State = #state{safe = false, publisher = Publisher, fn_id = FNId}) ->
   Publisher ! {publish, build_message(Item, State)},
   dataflow:maybe_debug(item_out, 1, Item, FNId, State#state.debug_mode),
   {ok, State}.
%% safe state
%%process(In, #data_batch{points = Points}, State = #state{}) ->
%%   [process(In, P, State) || P <- Points],
%%   {ok, State}.

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
   T = get_topic(Item, State),
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


