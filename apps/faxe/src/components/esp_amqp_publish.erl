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
-export([init/3, process/3, options/0, handle_info/2, shutdown/1]).

-define(DEFAULT_PORT, 5672).
-define(DEFAULT_SSL_PORT, 8883).

%% state for direct publish mode
-record(state, {
   client,
   connected = false,
   host,
   port,
   vhost,
   exchange,
   routing_key = false,
   ssl = false,
   opts,
   last_ticket
}).

options() -> [
   {host, binary},
   {port, integer, ?DEFAULT_PORT},
   {vhost, binary, <<"/">>},
   {routing_key, binary},
   {exchange, binary},
   {ssl, bool, false}].


init(_NodeId, _Ins,
   #{ host := Host0, port := _Port, vhost := _VHost, exchange := Ex,
      routing_key := RoutingKey, ssl := _UseSSL} = Opts0) ->

   Host = binary_to_list(Host0),
   Opts = Opts0#{host => Host},
   State = start_connection(#state{opts = Opts, exchange = Ex, routing_key = RoutingKey}),
   {ok, State}.

process(_In, Item, State = #state{client = Server, exchange = Exchange, routing_key = Key}) ->
   Payload = flowdata:to_json(Item),
   {ok, Ref} = bunny_worker:deliver(Server, {Exchange, Key, Payload, []}),
   {ok, State#state{last_ticket = Ref}}.

handle_info({publisher_ack, Ref}, State) ->
   lager:notice("message acked: ~p",[Ref]),
   {ok, State};
handle_info({'DOWN', _MonitorRef, process, Client, _Info}, #state{client = Client} = State) ->
   lager:notice("MQ-PubWorker ~p is 'DOWN'", [Client]),
   {ok, start_connection(State)}.

shutdown(#state{client = C}) ->
   catch (bunny_worker:stop(C)).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% internal
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
start_connection(State = #state{opts = Opts}) ->
   {ok, Pid} = bunny_worker:start(build_config(Opts)),
   erlang:monitor(process, Pid),
   State#state{client = Pid}.


-spec build_config(Opts :: map()) -> list().
build_config(_Opts = #{vhost := VHost, host := Host, port := Port}) ->
   HostParams = %% connection parameters
   [
      {hosts, [ {Host, Port} ]},
      {user, "admin"},
      {pass, "admin"},
      {reconnect_timeout, 2000},
      {ssl_options, none}, % Optional. Can be 'none' or [ssl_option()]
      {heartbeat, 60},
      {vhost, VHost}
   ],
   lager:warning("giving bunny_worker these Configs: ~p", [HostParams]),
   HostParams.


