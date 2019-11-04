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
   dt_field,
   dt_format,
   ssl = false,
   topics_seen = []
}).

options() -> [
   {host, binary}, {port, integer, ?DEFAULT_PORT},
   {qos, integer, 1},
   {topic, binary},
   {retained, is_set},
   {dt_field, string, <<"ts">>},
   {dt_format, string, ?TF_TS_MILLI},
   {ssl, bool, false}].


init(_NodeId, _Ins,
   #{ host := Host0, port := Port, topic := Topic, dt_field := DTField,
      dt_format := DTFormat,
      retained := Retained, ssl := UseSSL, qos := Qos} = _Opts) ->
   Host = binary_to_list(Host0),
   process_flag(trap_exit, true),
   erlang:send_after(0, self(), connect),
   State = #state{host = Host, port = Port, topic = Topic, dt_field = DTField, dt_format = DTFormat,
      retained = Retained, ssl = UseSSL, qos = Qos},
   connect(State),
   {ok, State}.

process(_In, _, State = #state{}) ->
   {ok, State}.


handle_info(connect, State) ->
   connect(State),
   {ok, State};
handle_info({mqttc, C, connected}, State=#state{}) ->
   lager:debug("mqtt client connected!!"),
   NewState = State#state{client = C, connected = true},
   subscribe(NewState),
   {ok, NewState};
handle_info({mqttc, _C,  disconnected}, State=#state{}) ->
   lager:debug("mqtt client disconnected!!"),
   {ok, State#state{connected = false, client = undefined}};
handle_info({publish, #{payload := Payload, topic := Topic} }, S=#state{dt_field = DTField, dt_format = DTFormat}) ->
   P0 = flowdata:from_json_struct(Payload, DTField, DTFormat),
   P = flowdata:set_field(P0, <<"topic">>, Topic),
   dataflow:emit(P),
   {ok, S};
handle_info({disconnected,shutdown,tcp_closed}=M, State = #state{}) ->
   lager:warning("emqtt : ~p", [M]),
   {ok, State};
handle_info({'EXIT', _C, _Reason}, State) ->
   lager:warning("EXIT emqtt"),
   connect(State),
   {ok, State};
handle_info(What, State) ->
   lager:warning("~p handle_info: ~p", [?MODULE, What]),
   {ok, State}.

shutdown(#state{client = C}) ->
   catch (emqtt:disconnect(C)).

connect(_State = #state{host = Host, port = Port}) ->
   {ok, Client} = emqtt:start_link([{host, Host}, {port, Port},{keepalive, 25}]),
   emqtt:connect(Client).

subscribe(#state{qos = Qos, client = C, topic = Topic}) when is_binary(Topic) ->
   {ok, _, _} = emqtt:subscribe(C, {Topic, Qos}).

