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

-define(DEFAULT_PORT, 1883).
-define(DEFAULT_SSL_PORT, 8883).
-define(DEFAULT_QUEUE_FILE, "/tmp/mqtt_q").

-record(state, {
   publisher,
   options,
   queue_file = ?DEFAULT_QUEUE_FILE,
   queue
}).



options() -> [
   {host, binary}, {port, integer, ?DEFAULT_PORT},
   {qos, integer, 1}, {topic, binary}, {retained, is_set},
   {ssl, bool, false}].

init(NodeId, _Ins, #{host := Host0}=Opts) ->
   lager:notice("++++ ~p ~ngot opts: ~p ~n",[NodeId, Opts]),
%%   emqttc:start_link([{host, Host}, {client_id, NodeId}, {logger, info}]),
   Host = binary_to_list(Host0),
   {ok, Q} = esq:new(?DEFAULT_QUEUE_FILE),
   {ok, Publisher} = mqtt_publisher:start_link(Opts#{host := Host}, Q),
   {ok, all, #state{options = Opts, publisher = Publisher, queue = Q}}.

process(_In, #data_batch{} = Batch, State = #state{queue = Q}) ->
   Json = flowdata:to_json(Batch),
   esq:enq(Json, Q),
   lager:info("enqueued: ~p :: ~p",[Json, Q]),
   {ok, State};
process(_Inport, #data_point{} = Point, State = #state{queue = Q}) ->
   Json = flowdata:to_json(Point),
   esq:enq(Json, Q),
   lager:info("enqueued: ~p :: ~p",[Json, Q]),
   {ok, State}.


handle_info(E, S) ->
   io:format("unexpected: ~p~n", [E]),
   {ok, S}.

shutdown(#state{publisher = P}) ->
   catch (P).

%%publish(Msg, #state{retained = Ret, qos = Qos, client = C, topic = Topic}) when is_binary(Msg) ->
%%   lager:notice("publish ~p on topic ~p ~n",[Msg, Topic]),
%%   emqttc:publish(C, Topic, Msg, [{qos, Qos}, {retained, Ret}]).

