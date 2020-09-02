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

-define(DEFAULT_PORT, 1883).
-define(DEFAULT_SSL_PORT, 8883).

%% state for safe-mode
-record(state, {
   publisher,
   options,
   queue_file,
   queue,
   topic,
   topic_lambda,
   safe = false,
   fn_id
}).
%% state for direct publish mode

options() -> [
   {host, binary, {mqtt, host}},
   {port, integer, {mqtt, port}},
   {user, string, {mqtt, user}},
   {pass, string, {mqtt, pass}},
   {client_id, string, {mqtt, client_id}},
   {qos, integer, 1},
   {topic, binary, undefined},
   {topic_lambda, lambda, undefined},
   {retained, is_set},
   {ssl, is_set, {mqtt, ssl, enable}},
   {safe, is_set, false}].

check_options() ->
   [
      {one_of_params, [topic, topic_lambda]}
   ].

metrics() ->
   [
      {?METRIC_SENDING_TIME, histogram, [slide, 60], "Network time for sending a message."},
      {?METRIC_BYTES_SENT, meter, [], "Size of item sent in kib."}
   ].

%% safe mode with ondisc queuing
init({GraphId, NodeId} = GId, _Ins, #{safe := true, host := Host0}=Opts) ->
   Host = binary_to_list(Host0),
   QFile = faxe_config:q_file(GId),
   {ok, Q} = esq:new(QFile, [{tts, 300}, {capacity, 10}]),
   {ok, Publisher} = mqtt_publisher:start_link(Opts#{host := Host, node_id => GId}, Q),
   init_all(Opts#{host := Host}, #state{publisher = Publisher, queue = Q, fn_id = GId});
%% direct publish mode
init(NodeId, _Ins, #{safe := false, host := Host0} = Opts) ->
   Host = binary_to_list(Host0),
   {ok, Publisher} = mqtt_publisher:start_link(Opts#{host := Host, node_id => NodeId}),
   init_all(Opts#{host := Host}, #state{publisher = Publisher, fn_id = NodeId}).

init_all(#{safe := Safe, topic := Topic, topic_lambda := LTopic} = Opts, State) ->
%%   lager:notice("mqtt SSLOpts: ~p" ,[SSLOpts]),
   {ok, all, State#state{options = Opts, safe = Safe, topic = Topic, topic_lambda = LTopic}}.


%% direct state
process(_In, #data_point{} = Point, State = #state{safe = true, queue = Q}) ->
   ok = esq:enq(build_message(Point, State), Q),
   {ok, State};
process(_Inport, #data_point{} = Point, State = #state{safe = false, publisher = Publisher}) ->
   Publisher ! {publish, build_message(Point, State)},
   {ok, State};
%% safe state
process(In, #data_batch{points = Points}, State = #state{}) ->
   [process(In, P, State) || P <- Points],
   {ok, State}.

handle_info(_E, S) ->
%%   lager:info()
   {ok, S}.

shutdown(#state{publisher = P}) ->
   catch gen_server:stop(P).

build_message(Point, State = #state{fn_id = FNId}) ->
   Json = flowdata:to_json(Point),
   node_metrics:metric(?METRIC_BYTES_SENT, byte_size(Json), FNId),
   node_metrics:metric(?METRIC_ITEMS_OUT, 1, FNId),
   {get_topic(Point, State), Json}.

get_topic(#data_point{} = _P, # state{topic_lambda = undefined, topic = Topic}) ->
   Topic;
get_topic(#data_point{} = P, #state{topic_lambda = Fun}) ->
   faxe_lambda:execute(P, Fun).

