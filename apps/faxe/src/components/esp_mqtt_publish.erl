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
   host,
   port,
   qos,
   topic,
   retained = false,
   ssl = false
}).

options() -> [
   {host, binary}, {port, integer, ?DEFAULT_PORT},
   {qos, integer, 1},
   {topic, binary},
   {retained, is_set},
   {ssl, bool, false},
   {save, is_set}].

%% save mode with ondisc queuing
init(NodeId, _Ins, #{save := true, host := Host0}=Opts) ->
   lager:notice("++++ ~p ~ngot opts: ~p ~n",[NodeId, Opts]),
%%   emqttc:start_link([{host, Host}, {client_id, NodeId}, {logger, info}]),
   Host = binary_to_list(Host0),
   {ok, Q} = esq:new(?DEFAULT_QUEUE_FILE),
   {ok, Publisher} = mqtt_publisher:start_link(Opts#{host := Host}, Q),
   {ok, all, #save_state{options = Opts, publisher = Publisher, queue = Q}};
%% direct publish mode
init(NodeId, _Ins,
   #{save := false, host := Host0, port := Port, topic := Topic,
      retained := Retained, ssl := UseSSL, qos := Qos} = Opts) ->
   lager:notice("++++ ~p ~ngot opts: ~p ~n",[NodeId, Opts]),
   Host = binary_to_list(Host0),
   {ok, Client} = emqtt:start_link([{host, Host}, {port, Port}]),
   emqtt:connect(Client),
   {ok,
      #direct_state{host = Host, port = Port, topic = Topic,
         retained = Retained, ssl = UseSSL, qos = Qos}}.

process(_In, #data_batch{} = Batch, State = #direct_state{}) ->
   Json = flowdata:to_json(Batch),
   publish(Json, State),
   {ok, State};
process(_Inport, #data_point{} = Point, State = #direct_state{}) ->
   Json = flowdata:to_json(Point),
   publish(Json, State),
   {ok, State};
process(_In, #data_batch{} = Batch, State = #save_state{queue = Q}) ->
   Json = flowdata:to_json(Batch),
   esq:enq(Json, Q),
   lager:info("enqueued: ~p :: ~p",[Json, Q]),
   {ok, State};
process(_Inport, #data_point{} = Point, State = #save_state{queue = Q}) ->
   Json = flowdata:to_json(Point),
   esq:enq(Json, Q),
   lager:info("enqueued: ~p :: ~p",[Json, Q]),
   {ok, State}.

handle_info({mqttc, C, connected}, State=#direct_state{}) ->
   lager:debug("mqtt client connected!!"),
   NewState = State#direct_state{client = C, connected = true},
   {ok, NewState};
handle_info({mqttc, _C,  disconnected}, State=#direct_state{}) ->
   lager:debug("mqtt client disconnected!!"),
   {ok, State#direct_state{connected = false, client = undefined}};
handle_info(E, S) ->
   lager:info("[~p] unexpected: ~p~n", [?MODULE, E]),
   {ok, S}.

shutdown(#save_state{publisher = P}) ->
   catch (P);
shutdown(#direct_state{client = C}) ->
   catch (emqtt:disconnect(C)).

publish(Msg, #direct_state{retained = Ret, qos = Qos, client = C, topic = Topic}) when is_binary(Msg) ->
   lager:notice("publish ~p on topic ~p ~n",[Msg, Topic]),
   {ok, PacketId} = emqtt:publish(C, Topic, Msg, [{qos, Qos}, {retained, Ret}]),
   lager:notice("sent msg and got PacketId: ~p",[PacketId]).

