-module(lager_emit_backend).

-behaviour(gen_event).

-include("faxe.hrl").
%% gen_event callbacks
-export([init/1
   , handle_call/2
   , handle_event/2
   , handle_info/2
   , terminate/2
   , code_change/3
   , start_trace/1, stop_trace/1]).

-record(state, { level  :: {mask, integer()}
   , fields :: [{atom(), jsx:json_term()}],
   writer_ready = false, mqtt_opts, storage_backend, publisher,
   flow_ids = [], topic
}).

-type init_error() :: undefined_host
| undefined_port
| {bad_fields, term()}
| {invalid_port, atom()}
| {failed_to_connect, inet:posix()}.

-type config() :: #{ port   := inet:port_number()
, host   := inet:hostname()
, level  := {mask, integer()}
, fields := [{atom(), jsx:json_term()}]
}.

-define(TOPIC_BASE, <<"ttgw/sys/faxe/">>).
%% delay the start of our mqtt publisher
-define(START_DELAY, 200).
-define(FLOW_LIST_UPDATE_INTERVAL, 5000).

%%==============================================================================
%% gen_event callbacks
%%==============================================================================

start_trace(FlowId) ->
   ets:insert(log_emit_flows, {FlowId, true}).

stop_trace(FlowId) ->
   ets:delete(log_emit_flows, FlowId).


init(Args) ->
   Level = proplists:get_value(level, Args, info),
   Options =
   case proplists:get_value(host, Args) of
      undefined -> faxe_config:get(mqtt);
      _O -> Args
   end,

%%   lager:info("options: ~p",[maps:from_list(Options)]),
   OptMap = maps:from_list(Options),
   MqttOpts = OptMap#{retained => false, qos => 1},

   Name = faxe_util:device_name(),
   BaseTopic = proplists:get_value(base_topic, Options, ?TOPIC_BASE),
   Topic = <<BaseTopic/binary, Name/binary, "/log/">>,

   erlang:send_after(?START_DELAY, self(), reconnect),
   erlang:send_after(?FLOW_LIST_UPDATE_INTERVAL, self(), update_flow_list),

   {ok, #state{level = Level, mqtt_opts = MqttOpts, topic = Topic}}.

handle_call(get_loglevel, State = #state{level = Level}) ->
   {ok, Level, State};
handle_call({set_loglevel, Level}, State) ->
   {ok, ok, State#state{level = lager_util:config_to_mask(Level)}};
handle_call(_Request, State) ->
   {ok, ok, State}.
handle_event({log, _M}, State = #state{flow_ids = []}) ->
   {ok, State};
handle_event({log, _M}, State = #state{publisher = undefined}) ->
   {ok, State};
handle_event({log, Message}, State = #state{level = Level, flow_ids = Flows}) ->

   case lager_util:is_loggable(Message, Level, ?MODULE) of
      true ->
         %% we only log messages concerning dataflows
         MetaData = lager_msg:metadata(Message),
         case proplists:get_value(flow, MetaData) of
            undefined -> ok;
            FlowId ->
               case lists:member(FlowId, Flows) of
                  true ->
                     NodeId = proplists:get_value(comp, MetaData),
                     publish(FlowId, NodeId, Message, State);
                  false ->
                     ok
               end
         end;
      false ->
         ok
   end,
   {ok, State};
handle_event(_Event, State) ->
   {ok, State}.

handle_info(update_flow_list, State = #state{}) ->
   List = proplists:get_keys(ets:tab2list(log_emit_flows)),
%%   lager:info("new flow_list: ~p",[List]),
   erlang:send_after(?FLOW_LIST_UPDATE_INTERVAL, self(), update_flow_list),
   {ok, State#state{flow_ids = List}};
handle_info(reconnect, State = #state{mqtt_opts = MqttOpts}) ->
   {ok, Publisher} = mqtt_publisher:start_link(MqttOpts),
   {ok, State#state{publisher = Publisher}};
handle_info({add_flow, FlowId}, State = #state{flow_ids = Flows}) ->
%%   lager:info("add_flow: ~p", [FlowId]),
   case lists:member(FlowId, Flows) of
      true -> {ok, State};
      false -> {ok, State#state{flow_ids = [FlowId|Flows]}}
   end;
handle_info({remove_flow, FlowId}, State = #state{flow_ids = Flows}) ->
%%   lager:info("remove_flow: ~p", [FlowId]),
   {ok, State#state{flow_ids = lists:delete(FlowId, Flows)}};

handle_info(_, State) ->
   {ok, State}.

terminate(_, #state{}) -> ok.

code_change(_, State, _) ->
   {ok, State}.

%%==============================================================================
%% Internal functions
%%==============================================================================
publish(_F, undefined, _, _S) -> ok;
publish(FlowId, NodeId, Message, #state{publisher = Publisher, topic = T}) ->
   Topic = <<T/binary, FlowId/binary, "/", NodeId/binary>>,
%%   lager:notice("publish: ~p~n on topic : ~p",[flowdata:to_json(format_data(Message)), Topic]),
   Publisher ! {publish, {Topic, flowdata:to_json(format_data(Message))}}.


%%-spec format_data(Message) -> Data when
%%   Message :: lager_msg:lager_msg(),
%%   Fields  :: [{atom(), jsx:json_term()}],
%%   Data    :: jsx:json_term().
format_data(Message) ->
   Metadata = lager_msg:metadata(Message),
   Flow = proplists:get_value(flow, Metadata, null),
   Comp = proplists:get_value(comp, Metadata, null),
   M0 = proplists:delete(flow, Metadata),
   M1 = proplists:delete(comp, M0),
%%   io:format("~nMetaData: ~p~n",[Metadata]),
   AllFieldsMeta = safe_fields(M1),
   MetaMap = maps:from_list(AllFieldsMeta),
   D = #{
%%      <<"ts">> => format_timestamp(lager_msg:timestamp(Message)),
      <<"level">> => lager_msg:severity(Message),
      <<"flow_id">> => Flow,
      <<"node_id">> => Comp,
      <<"message">> => unicode:characters_to_binary(lager_msg:message(Message)),
      <<"meta">> => MetaMap
   },
   #data_point{ts = format_timestamp(lager_msg:timestamp(Message)), fields = D}.

-spec safe_fields([{term(), term()}]) -> [{atom() | binary(), jsx:json_term()}].
safe_fields(KVPairs) ->
   lists:map(fun safe_field/1, KVPairs).

-spec safe_field({term(), term()}) -> {atom() | binary(), jsx:json_term()}.
safe_field({Key, Value}) when is_atom(Key);
   is_binary(Key)->
   {Key, safe_value(Value)};
safe_field({Key, Value}) when is_list(Key) ->
   safe_field({list_to_binary(Key), Value}).

-spec safe_value(term()) -> jsx:json_term().
safe_value(Pid) when is_pid(Pid) ->
   list_to_binary(pid_to_list(Pid));
safe_value(List) when is_list(List) ->
   case io_lib:char_list(List) of
      true ->
         list_to_binary(List);
      false ->
         lists:map(fun safe_value/1, List)
   end;
safe_value(Val) ->
   Val.

-spec format_timestamp(erlang:timestamp()) -> binary().
format_timestamp(Ts = {_, _, _Ms}) ->
   {_, _, Micro} = Ts,
   {Date, {Hours, Minutes, Seconds}} = calendar:now_to_universal_time(Ts),
   MsDateTime = {Date, {Hours, Minutes, Seconds, Micro div 1000 rem 1000}},
   faxe_time:to_ms(MsDateTime).

