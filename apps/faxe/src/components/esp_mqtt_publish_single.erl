%%%-------------------------------------------------------------------
%%% @author heyoka
%%% @copyright (C) 2019, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 27. May 2019 09:00
%%%-------------------------------------------------------------------
-module(esp_mqtt_publish_single).
-author("heyoka").

%% API
-behavior(df_component).

-include("faxe.hrl").
%% API
-export([init/3, process/3, options/0, handle_info/2, shutdown/1]).

-record(state, {
   client,
   connected = false,
   host,
   port,
   qos,
   topic,
   retained = false,
   ssl = false
}).

-define(DEFAULT_PORT, 1883).
-define(DEFAULT_SSL_PORT, 8883).

options() -> [
   {host, binary}, {port, integer, ?DEFAULT_PORT},
   {qos, integer, 1}, {topic, binary}, {retained, is_set},
   {ssl, bool, false}].

init(NodeId, _Ins, #{host := Host0, port := Port, topic := Topic, qos := Qos, retained := Retained, ssl := UseSSL}=Opts) ->
   lager:notice("++++ ~p ~ngot opts: ~p ~n",[NodeId, Opts]),
%%   emqttc:start_link([{host, Host}, {client_id, NodeId}, {logger, info}]),
   Host = binary_to_list(Host0),
   emqttc:start_link([{host, Host}, {port, Port}, {logger, info}]),
   {ok, all, #state{host = Host, port = Port, topic = Topic, retained = Retained, ssl = UseSSL, qos = Qos}}.

process(_In, #data_batch{} = Batch, State = #state{}) ->
   Json = flowdata:to_json(Batch),
   publish(Json, State),
   {ok, State};
process(_Inport, #data_point{} = Point, State = #state{}) ->
   Json = flowdata:to_json(Point),
   publish(Json, State),
   {ok, State}.

handle_info({mqttc, C, connected}, State=#state{}) ->
   lager:warning("mqtt client connected!!"),
   {ok, State#state{client = C, connected = true}};
handle_info({mqttc, _C,  disconnected}, State=#state{}) ->
   lager:warning("mqtt client disconnected!!"),
   {ok, State#state{connected = false, client = undefined}};
handle_info(E, S) ->
   io:format("unexpected: ~p~n", [E]),
   {ok, S}.

shutdown(#state{client = C}) ->
   catch (emqttc:disconnect(C)).

publish(Msg, #state{retained = Ret, qos = Qos, client = C, topic = Topic}) when is_binary(Msg) ->
   lager:notice("publish ~p on topic ~p ~n",[Msg, Topic]),
   emqttc:publish(C, Topic, Msg, [{qos, Qos}, {retained, Ret}]).
