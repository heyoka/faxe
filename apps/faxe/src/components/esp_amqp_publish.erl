%%%-------------------------------------------------------------------
%%% @author heyoka
%%% @copyright (C) 2019, <COMPANY>
%%% @doc
%%% Publish data to an amqp-broker like rabbitmq.
%%%
%%%
%%% port() is 5672 default
%%% ssl() is false by default
%%% @end
%%% Created : 10. October 2019 11:17
%%%-------------------------------------------------------------------
-module(esp_amqp_publish).
-author("heyoka").

%% API
-behavior(df_component).

-include("faxe.hrl").
%% API
-export([init/3, process/3, options/0, handle_info/2, shutdown/1, metrics/0, check_options/0]).

%% state for direct publish mode
-record(state, {
   client,
   connected = false,
   host,
   port,
   vhost,
   exchange,
   routing_key = false,
   rk_lambda = undefined,
   ssl = false,
   opts,
   queue = undefined,
   flowid_nodeid,
   debug_mode = false,
   safe_mode = false,
   use_internal_queue = true
}).

options() -> [
   {host, string, {amqp, host}},
   {port, integer, {amqp, port}},
   {user, string, {amqp, user}},
   {pass, string, {amqp, pass}},
   {vhost, string, <<"/">>},
   {routing_key, string, undefined},
   {routing_key_lambda, lambda, undefined},
   {exchange, string, {rabbitmq, root_exchange}},
   {ssl, is_set, false},
   {persistent, bool, false},

   {qos, integer, 1}
].

check_options() ->
   [{one_of_params, [routing_key, routing_key_lambda]},
      {one_of, qos, [0, 1, 2]}].

metrics() ->
   [
      {?METRIC_BYTES_SENT, meter, []}
   ].

init({_GraphId, _NodeId} = Idx, _Ins,
   #{ host := Host0, port := Port, user := _User, pass := _Pass, vhost := _VHost, exchange := Ex,
      routing_key := RoutingKey, routing_key_lambda := RkLambda, ssl := _UseSSL,
      persistent := _Persist} = Opts0) ->

   Opts1 = #{safe_mode := SafeMode, use_queue := UseInternalQueue} = eval_qos(Opts0),

   process_flag(trap_exit, true),
   Host = binary_to_list(Host0),
   Opts = Opts1#{host => Host},

   State0 = #state{opts = Opts, exchange = Ex, routing_key = RoutingKey, flowid_nodeid = Idx,
      rk_lambda = RkLambda, safe_mode = SafeMode, use_internal_queue = UseInternalQueue},

   State1 = maybe_start_queue(State0),

   connection_registry:reg(Idx, Host, Port, <<"amqp">>),
   connection_registry:connecting(),
   State = start_connection(State1),
   {ok, State}.

eval_qos(Opts = #{qos := 0}) ->
   Opts#{safe_mode => false, use_queue => false};
eval_qos(Opts = #{qos := 1}) ->
   Opts#{safe_mode => false, use_queue => true};
eval_qos(Opts = #{qos := 2}) ->
   Opts#{safe_mode => true, use_queue => true}.

process(_In, Item, State = #state{flowid_nodeid = FNId, debug_mode = Debug}) ->
   Payload = flowdata:to_json(Item),
   node_metrics:metric(?METRIC_BYTES_SENT, byte_size(Payload), FNId),
   node_metrics:metric(?METRIC_ITEMS_OUT, 1, FNId),
   handle_data(key(Item, State), Payload, State),
   dataflow:maybe_debug(item_out, 1, Item, FNId, Debug),
   {ok, State}.

handle_info(start_debug, State) -> {ok, State#state{debug_mode = true}};
handle_info(stop_debug, State) -> {ok, State#state{debug_mode = false}};
handle_info({publisher_ack, Ref}, State) ->
   lager:info("message acked: ~p",[Ref]),
   {ok, State};
handle_info({'DOWN', _MonitorRef, process, Client, _Info}, #state{client = Client} = State) ->
   connection_registry:disconnected(),
   lager:notice("MQ-PubWorker ~p is 'DOWN' Info:~p", [Client, _Info]),
   {ok, start_connection(State)};
handle_info(Other, #state{client = Client} = State) ->
   lager:warning("Process ~p is 'DOWN' Info:~p, client: ~p", [Client, Other]),
   {ok, State}.

shutdown(#state{client = C, queue = Q}) ->
   catch (bunny_esq_worker:stop(C)),
   catch (exit(Q)).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% internal
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
maybe_start_queue(State = #state{use_internal_queue = false}) ->
   State;
maybe_start_queue(State = #state{safe_mode = Safe, flowid_nodeid = Idx}) ->
   %% config and start queue
   QFile = faxe_config:q_file(Idx),
   QOpts0 = faxe_config:get_esq_opts(),
   QOpts = case Safe of true -> QOpts0; false -> proplists:delete(ttf, QOpts0) end,
   {ok, Q} = esq:new(QFile, QOpts),
   State#state{queue = Q}.

handle_data(Key, Payload, #state{use_internal_queue = true, exchange = Exchange, queue = Q}) ->
   ok = esq:enq({Exchange, Key, Payload, []}, Q);
handle_data(Key, Payload, #state{use_internal_queue = false, client = Publisher, exchange = Exchange}) ->
   Publisher ! {deliver, Exchange, Key, Payload, []}.

start_connection(State = #state{opts = Opts, queue = Q}) ->
   {ok, Pid} = bunny_esq_worker:start(Q, Opts),
   erlang:monitor(process, Pid),
   connection_registry:connected(),
   State#state{client = Pid}.

key(#data_point{} = _P, #state{rk_lambda = undefined, routing_key = Topic}) ->
   Topic;
key(#data_point{} = P, #state{rk_lambda = Fun}) ->
   faxe_lambda:execute(P, Fun).
