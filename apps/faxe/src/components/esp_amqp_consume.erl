%%%-------------------------------------------------------------------
%%% @author heyoka
%%% @copyright (C) 2019, <COMPANY>
%%% @doc
%%% Consume data from an amqp-broker like rabbitmq.
%%%
%%% port() is 7062 default
%%% ssl() is false by default
%%% @end
%%% Created : 27. May 2019 09:00
%%%-------------------------------------------------------------------
-module(esp_amqp_consume).
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
   consumer,
   connected = false,
   host,
   port,
   vhost,
   queue,
   exchange,
   routing_key = false,
   prefetch,
   ssl = false
}).

options() -> [
   {host, binary},
   {port, integer, ?DEFAULT_PORT},
   {vhost, binary, <<"/">>},
   {routing_key, binary},
   {queue, binary},
   {exchange, binary},
   {prefetch, integer, 1},
   {ssl, bool, false}].


init(_NodeId, _Ins,
   #{ host := Host0, port := Port, vhost := VHost, queue := Q, exchange := Ex, prefetch := Prefetch,
      routing_key := RoutingKey, ssl := UseSSL} = Opts) ->
   Host = binary_to_list(Host0),
   {ok, Pid, _NewConsumer} = rmq_consumer:start_monitor(self(), consumer_config(Opts)),
   {ok,
      #state{host = Host, port = Port, vhost = VHost, queue = Q, prefetch = Prefetch,
         routing_key = RoutingKey, exchange = Ex, ssl = UseSSL, consumer = Pid}}.

process(_In, _, State = #state{}) ->
   {ok, State}.

%%
%% new queue-message arrives ...
%%
handle_info({ {DTag, RKey}, {Payload, _Headers}, From}, State) ->
   process(RKey, Payload),
   carrot:ack(From, DTag),
   {ok, State};
%%handle_info({ {#'basic.deliver'{delivery_tag = DTag, routing_key = RKey},
%%   #'amqp_msg'{payload = Payload, props = #'P_basic'{headers = _Headers}}}, From}, State) ->
%%   process(RKey, Payload),
%%   carrot:ack(From, DTag),
%%   {ok, State};
handle_info({'DOWN', _MonitorRef, process, Consumer, _Info}, #state{consumer = Consumer} = State) ->

   lager:notice("MQ-Consumer ~p is 'DOWN'",[Consumer]),
   {ok, Pid, NewConsumer} = rmq_consumer:start_monitor(self(), consumer_config(State)),
   {ok, State#state{}}.

shutdown(#state{consumer = C}) ->
   catch (emqtt:disconnect(C)).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% internal
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec consumer_config(Opts :: map()) -> list().
consumer_config(_Opts = #{vhost := _VHost, queue := Q, prefetch := Prefetch,
   exchange := XChange, routing_key := RoutingKey}) ->

   {ok, HostParams} = application:get_env(carrot, broker),
   Config =
      [
         {workers, 1},  % Number of connections, but not relevant here,
         % because we start the consumer monitored
         {callback, self()},
         {setup_type, permanent},
         {prefetch_count, Prefetch},
         {setup,
            [
               {queue, [
                  {queue, Q},
                  {exchange, XChange},
                  {routing_key, RoutingKey}
%%                     ,
%%                  {bindings, StreamIds}

               ]}
            ]
         }


      ],
   carrot_util:proplists_merge(HostParams, Config).

process(RKey, Payload) ->
   Msg0 = flowdata:from_json(Payload),
   DataPoint = Msg0#data_point{id = RKey},
   dataflow:emit(DataPoint).


