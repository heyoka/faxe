%%%-------------------------------------------------------------------
%%% @author heyoka
%%% @copyright (C) 2019, <COMPANY>
%%% @doc
%%% Publish every single message to a mqtt-broker.
%%% Incoming data_points or data_batchs are converted to a JSON string before sending.
%%% If the safe() parameter is given, every message first gets stored in an ondisk queue before it will
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
-export([init/3, process/3, options/0, handle_info/2, shutdown/1]).

-define(DEFAULT_PORT, 1883).
-define(DEFAULT_SSL_PORT, 8883).
-define(ESQ_BASE_DIR, <<"/tmp/">>).

%% state for safe-mode
-record(state, {
   publisher,
   options,
   queue_file,
   queue,
   topic,
   topic_lambda,
   safe = false
}).
%% state for direct publish mode

options() -> [
   {host, binary, {mqtt, host}},
   {port, integer, {mqtt, port}},
   {qos, integer, 1},
   {topic, binary, undefined},
   {topic_lambda, lambda, undefined},
   {retained, is_set},
   {ssl, is_set},
   {safe, is_set}].

%% safe mode with ondisc queuing
init({GraphId, NodeId}, _Ins, #{safe := true, host := Host0}=Opts) ->
   Host = binary_to_list(Host0),
   lager:info("NodeId is : ~p", [NodeId]),
   QFile = binary_to_list(<<?ESQ_BASE_DIR/binary, GraphId/binary, "/", NodeId/binary>>),
   {ok, Q} = esq:new(QFile, [{tts, 300}, {capacity, 10}]),
   {ok, Publisher} = mqtt_publisher:start_link(Opts#{host := Host}, Q),
   init_all(Opts#{host := Host}, #state{publisher = Publisher, queue = Q});
%% direct publish mode
init(_NodeId, _Ins, #{safe := false, host := Host0} = Opts) ->
   Host = binary_to_list(Host0),
   {ok, Publisher} = mqtt_publisher:start_link(Opts#{host := Host}),
   init_all(Opts#{host := Host}, #state{publisher = Publisher}).

init_all(#{safe := Safe, topic := Topic, topic_lambda := LTopic} = Opts, State) ->
   {ok, all, State#state{options = Opts, safe = Safe, topic = Topic, topic_lambda = LTopic}}.

%% direct state
process(_In, #data_point{} = Point, State = #state{safe = true, queue = Q}) ->
   esq:enq(build_message(Point, State), Q),
   {ok, State};
process(_Inport, #data_point{} = Point, State = #state{safe = false, publisher = Publisher}) ->
   Publisher ! {publish, build_message(Point, State)},
   {ok, State};
%% safe state
process(In, #data_batch{points = Points}, State = #state{}) ->
   [process(In, P, State) || P <- Points],
   {ok, State}.

handle_info(_E, S) ->
   {ok, S}.

shutdown(#state{publisher = P}) ->
   catch exit(P, stop).

build_message(Point, State) ->
   Json = flowdata:to_json(Point),
   {get_topic(Point, State), Json}.

get_topic(#data_point{} = _P, # state{topic_lambda = undefined, topic = Topic}) ->
   Topic;
get_topic(#data_point{} = P, #state{topic_lambda = Fun}) ->
   faxe_lambda:execute(P, Fun).

