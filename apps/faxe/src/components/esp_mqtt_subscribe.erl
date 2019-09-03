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
-module(esp_mqtt_subscribe).
-author("heyoka").

%% API
-behavior(df_component).

-include("faxe.hrl").
%% API
-export([init/3, process/3, options/0, handle_info/2, shutdown/1]).

-define(DEFAULT_PORT, 1883).
-define(DEFAULT_SSL_PORT, 8883).

%% state for direct publish mode
-record(state, {
   client,
   connected = false,
   host,
   port,
   qos,
   topic,
   retained = false,
   ssl = false,
   topics_seen = []
}).

options() -> [
   {host, binary}, {port, integer, ?DEFAULT_PORT},
   {qos, integer, 1},
   {topic, binary},
   {retained, is_set},
   {ssl, bool, false}].


init(NodeId, _Ins,
   #{ host := Host0, port := Port, topic := Topic,
      retained := Retained, ssl := UseSSL, qos := Qos} = Opts) ->
   lager:notice("++++ ~p ~ngot opts: ~p ~n",[NodeId, Opts]),
   Host = binary_to_list(Host0),
   {ok, Client} = emqtt:start_link([{host, Host}, {port, Port}]),
   emqtt:connect(Client),
   {ok,
      #state{host = Host, port = Port, topic = Topic,
         retained = Retained, ssl = UseSSL, qos = Qos}}.

process(_In, _, State = #state{}) ->
   {ok, State}.

handle_info({mqttc, C, connected}, State=#state{}) ->
   lager:debug("mqtt client connected!!"),
   NewState = State#state{client = C, connected = true},
   subscribe(NewState),
   {ok, NewState};
handle_info({mqttc, _C,  disconnected}, State=#state{}) ->
   lager:debug("mqtt client disconnected!!"),
   {ok, State#state{connected = false, client = undefined}};
handle_info({publish, #{payload := Payload, topic := Topic} }, S=#state{topics_seen = Seen}) ->
%%   P0 = flowdata:set_field(#data_point{ts = faxe_time:now()}, <<"topic">>, Topic),
%%   P = flowdata:set_field(P0, <<"payload">>, Payload),
%%   dataflow:emit(P),
%%%%   lager:info("[~p] message: ~p~n", [Topic, Payload]),
%%   {ok, S};
Seen1 =
case lists:member(Topic, Seen) of
true -> Seen;
false -> P0 = #data_point{ts = faxe_time:now()},
P = flowdata:set_field(P0, <<"topics_seen">>, Topic),
dataflow:emit(P),
[Topic|Seen]
end,
{ok, S#state{topics_seen = Seen1}};
handle_info(E, S) ->
   lager:info("[~p] message: ~p~n", [?MODULE, E]),
   {ok, S}.

shutdown(#state{client = C}) ->
   catch (emqtt:disconnect(C)).

subscribe(#state{qos = Qos, client = C, topic = Topic}) when is_binary(Topic) ->
   lager:notice("subscribe to topic ~p ~n",[Topic]),
   {ok, _, _} = emqtt:subscribe(C, {Topic, Qos}).

