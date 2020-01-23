%%%-------------------------------------------------------------------
%%% @author heyoka
%%% @copyright (C) 2019, <COMPANY>
%%% @doc
%%% Publish every single message to a mqtt-broker.
%%% Incoming data_points or data_batchs are converted to a JSON string before sending.
%%% If the save() parameter is given, every message first gets stored in an ondisk queue before it will
%%% be sent, this way we can make sure no message gets lost when disconnected from the broker.
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
-define(DEFAULT_QUEUE_FILE, "/tmp/mqtt_q").

%% state for save-mode
-record(save_state, {
   publisher,
   options,
   queue_file = ?DEFAULT_QUEUE_FILE,
   queue
}).
%% state for direct publish mode
-record(direct_state, {
   client,
   connected = false,
   reconnector :: faxe_backoff:backoff(),
   host,
   port,
   qos,
   topic,
   topic_lambda,
   queue,
   retained = false,
   ssl = false,
   ssl_opts = []
}).

options() -> [
   {host, binary}, {port, integer, ?DEFAULT_PORT},
   {qos, integer, 1},
   {topic, binary, undefined},
   {topic_lambda, lambda, undefined},
   {retained, is_set},
   {ssl, is_set},
   {save, is_set}].

%% save mode with ondisc queuing
init(_NodeId, _Ins, #{save := true, host := Host0}=Opts) ->
   Host = binary_to_list(Host0),
   {ok, Q} = esq:new(?DEFAULT_QUEUE_FILE),
   {ok, Publisher} = mqtt_publisher:start_link(Opts#{host := Host}, Q),
   {ok, all, #save_state{options = Opts, publisher = Publisher, queue = Q}};
%% direct publish mode
init(_NodeId, _Ins,
   #{save := false, host := Host0, port := Port, topic := Topic, topic_lambda := LTopic,
      retained := Retained, ssl := UseSsL, qos := Qos}) ->
   process_flag(trap_exit, true),
   Host = binary_to_list(Host0),
   reconnect_watcher:new(10000, 5, io_lib:format("~s:~p ~p",[Host, Port, ?MODULE])),
   Reconnector = faxe_backoff:new({5, 1200}),
   {ok, Reconnector1} = faxe_backoff:execute(Reconnector, reconnect),
   {ok,
      #direct_state{host = Host, port = Port, topic = Topic, topic_lambda = LTopic,
         connected = false, ssl_opts = [], reconnector = Reconnector1,
         retained = Retained, ssl = UseSsL, qos = Qos, queue = queue:new()}}.

%% direct state
process(_In, Item, State = #direct_state{connected = true}) ->
%%   lager:alert("connected!!"),
   Json = flowdata:to_json(Item),
   publish(Json, get_topic(Item, State), State),
   {ok, State};
process(_Inport, Item, State = #direct_state{connected = false, queue = Q}) ->
%%   lager:alert("not connected!!"),
   Json = flowdata:to_json(Item),
   Q1 = queue:in({get_topic(Item, State), Json}, Q),
   {ok, State#direct_state{queue = Q1}};
%% save state
process(_In, #data_batch{} = Batch, State = #save_state{queue = Q}) ->
   Json = flowdata:to_json(Batch),
   esq:enq(Json, Q),
   {ok, State};
process(_Inport, #data_point{} = Point, State = #save_state{queue = Q}) ->
   Json = flowdata:to_json(Point),
   esq:enq(Json, Q),
   {ok, State}.

handle_info({mqttc, C, connected}, State=#direct_state{queue = Q}) ->
   lager:info("mqtt client connected, resend : ~p!!",[queue:to_list(Q)]),
   PendingList = queue:to_list(Q),
   NewState = State#direct_state{client = C, connected = true, queue = queue:new()},
   [publish(J, Topic, NewState) || {Topic, J} <- PendingList],
   {ok, NewState};
handle_info({mqttc, _C,  disconnected}, State=#direct_state{client = _Client}) ->
%%   catch exit(Client),
   lager:warning("mqtt client disconnected!!"),
   {ok, State#direct_state{connected = false, client = undefined}};
handle_info(reconnect, State = #direct_state{}) ->
   NewState = do_connect(State),
   {ok, NewState};
handle_info({'EXIT', _Client, Reason},
    State = #direct_state{reconnector = Recon, host = H, port = P}) ->
   lager:notice("MQTT Client exit: ~p ~p", [Reason, {H, P}]),
   {ok, Reconnector} = faxe_backoff:execute(Recon, reconnect),
   {ok, State#direct_state{connected = false, client = undefined, reconnector = Reconnector}};
handle_info(_E, S) ->
   {ok, S}.

shutdown(#save_state{publisher = P}) ->
   catch (P);
shutdown(#direct_state{client = C}) ->
   lager:debug("shutdown: ~p", [?MODULE]),
   catch (emqttc:disconnect(C)).


%%publish(Msg, State = #direct_state{topic = Topic}) ->
%%   publish(Msg, Topic, State).

publish(Msg, Topic, #direct_state{retained = Ret, qos = Qos, client = C})
   when is_binary(Msg); is_list(Msg) ->
   lager:notice("publish ~s on topic ~p ~n",[Msg, Topic]),
%%   {ok, _PacketId}
   ok = emqttc:publish(C, Topic, Msg, [{qos, Qos}, {retain, Ret}])
%%   ,
%%   lager:notice("sent msg and got PacketId: ~p",[PacketId])
.

do_connect(#direct_state{host = Host, port = Port, ssl = _Ssl} = State) ->
   reconnect_watcher:bump(),
   Opts = [{host, Host}, {port, Port}, {keepalive, 30}],
   {ok, _Client} = emqttc:start_link(Opts),
   State.
%%   ,
%%   case catch(emqtt:connect(Client)) of
%%      {ok, _} -> State#direct_state{client = Client, connected = true};
%%      _Other ->
%%         exit(Client)
%%         lager:notice("Error connecting to mqtt_broker: ~p ~p", [{Host, Port}, _Other]),
%%         erlang:send_after(2000, self(), reconnect), State
%%   end.

get_topic(#data_point{} = _P, #direct_state{topic_lambda = undefined, topic = Topic}) ->
   Topic;
get_topic(#data_point{} = P, #direct_state{topic_lambda = Fun}) ->
   faxe_lambda:execute(P, Fun).

