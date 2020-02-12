%%%-------------------------------------------------------------------
%%% @author heyoka
%%% @copyright (C) 2019, <COMPANY>
%%% @doc
%%% Consume data from an amqp-broker like rabbitmq.
%%% When prefetch is given and is > 1, then this node will emit a data_batch record instead of a data_point
%%%
%%% port() is 5672 default
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

-define(DEFAULT_PORT, 5672).
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
   ssl = false,
   opts,
   dt_field,
   dt_format,
   collected = 0,
   points = queue:new()
}).

options() -> [
   {host, binary, {amqp, host}},
   {port, integer, {amqp, port}},
   {ssl, bool, false},
   {vhost, binary, <<"/">>},
   {routing_key, binary},
   {queue, binary},
   {exchange, binary},
   {prefetch, integer, 1},
   {dt_field, string, <<"ts">>},
   {dt_format, string, ?TF_TS_MILLI}
].


init(_NodeId, _Ins,
   #{ host := Host0, port := _Port, vhost := _VHost, queue := _Q,
      exchange := _Ex, prefetch := Prefetch, routing_key := _RoutingKey, dt_field := DTField,
      dt_format := DTFormat, ssl := _UseSSL}
      = Opts0) ->

   Host = binary_to_list(Host0),
   Opts = Opts0#{host => Host},
   State = start_consumer(#state{opts = Opts, prefetch = Prefetch,
      dt_field = DTField, dt_format = DTFormat}),
   {ok, State}.

process(_In, _, State = #state{}) ->
   {ok, State}.

%%
%% new queue-message arrives ...
%%
handle_info({ {DTag, RKey}, {Payload, _Headers}, From}, State=#state{prefetch = 1}) ->
   DataPoint = build_point(Payload, RKey, State#state.dt_field, State#state.dt_format),
   dataflow:emit(DataPoint),
   carrot:ack(From, DTag),
   {ok, State};

handle_info({ {DTag, RKey}, {Payload, _Headers}, From},
    State=#state{collected = NumCollected, points = Batch}) ->
   DataPoint = build_point(Payload, RKey, State#state.dt_field, State#state.dt_format),
   NewState = State#state{points = queue:in(DataPoint, Batch), collected = NumCollected+1},
   maybe_emit(DTag, From, NewState);

handle_info({'DOWN', _MonitorRef, process, Consumer, _Info}, #state{consumer = Consumer} = State) ->
   lager:notice("MQ-Consumer ~p is 'DOWN'",[Consumer]),
   {ok, start_consumer(State)}.

shutdown(#state{consumer = C}) ->
   catch (rmq_consumer:stop(C)).

maybe_emit(DTag, From, State = #state{prefetch = Prefetch, collected = Prefetch, points = Points}) ->
   DataBatch = #data_batch{points = queue:to_list(Points)},
   NewState = State#state{points = queue:new(), collected = 0},
   lager:notice("emit: ~p",[Prefetch]),
   dataflow:emit(DataBatch),
   carrot:ack_multiple(From, DTag),
   {ok, NewState};
maybe_emit(_DTag, _From, State = #state{}) ->
   {ok, State}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% internal
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
build_point(Payload, RKey, DTField, DTFormat) ->
   Msg0 = flowdata:from_json_struct(Payload, DTField, DTFormat),
   flowdata:set_field(Msg0, <<"topic">>, RKey).

start_consumer(State = #state{opts = ConsumerOpts}) ->
   {ok, Pid, _NewConsumer} =
      rmq_consumer:start_monitor(self(), consumer_config(ConsumerOpts)),
   State#state{consumer = Pid}.


-spec consumer_config(Opts :: map()) -> list().
consumer_config(_Opts = #{vhost := VHost, queue := Q, prefetch := Prefetch,
   exchange := XChange, routing_key := RoutingKey, host := Host, port := Port}) ->

   HostParams = %% connection parameters
   [
      {hosts, [ {Host, Port} ]},
      {user, "admin"},
      {pass, "admin"},
      {reconnect_timeout, 2000},
      {ssl_options, none} % Optional. Can be 'none' or [ssl_option()]
   ],
   Config =
      [
         {workers, 1},  % Number of connections, but not relevant here,
         % because we start the consumer monitored
         {callback, self()},
         {setup_type, permanent},
         {prefetch_count, Prefetch},
         {vhost, VHost},
         {setup,
            [
               {queue, [
                  {queue, Q},
                  {exchange, XChange},
                  {routing_key, RoutingKey}
%%                     ,
%%                  {bindings, StreamIds}

               ]},
               {exchange, [
                         {exchange, XChange},
                         {type, <<"topic">>},
                         {source, <<"x_lm_fanout">>}
                         ]
                      }
            ]
         }


      ],
   Props = carrot_util:proplists_merge(HostParams, Config),
   lager:warning("giving carrot these Configs: ~p", [Props]),
   Props.


