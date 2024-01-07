%%%-------------------------------------------------------------------
%%% @author heyoka
%%% @copyright (C) 2023
%%% @doc
%%% @end
%%%-------------------------------------------------------------------
-module(faxe_flow_observer).

-behaviour(gen_server).

-include("faxe.hrl").

-export([start_link/1, is_alive/1, get_observer/1, stop/1, start_monitor/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
  code_change/3]).

-define(ETS_FLOW_OBSERVER_TABLE, flow_observer).
-define(BASE_TOPIC, <<"tgw/sys/faxe">>).
-define(TOPIC_KEY, <<"health">>).
-define(QOS, 1).
-define(REPORT_INTERVAL, proplists:get_value(report_interval, faxe_config:get_sub(flow_health, observer), 10000)).
-define(BUFFER_MAX_AGE, round(?REPORT_INTERVAL/2)).

-define(CONN_REF_FIELDS, [<<"conn_type">>, <<"peer">>, <<"port">>]).
-define(CONN_FIELD_CONNECTED, <<"connected">>).
-define(CONN_FIELD_STATUS, <<"status">>).

-define(MSG_HEALTHY, #{<<"status">> => 1}).
-define(MSG_UNHEALTHY, #{<<"status">> => 0}).
-define(MSG_STOPPED, #{<<"status">> => 2}).
-define(MSG_CRASHED, #{<<"status">> => 3}).
-define(FIELD_CONN_STATUS, <<"conn_status">>).
-define(FIELD_ERRORS, <<"processing_errors">>).
-define(FIELD_MESSAGE, <<"message">>).

-record(state, {
  flow_id                 :: binary(),
  graph                   :: pid(),
  topic                   :: binary(),
  mqtt_connected = false  :: true | false,
  host                    :: string(),
  timer_ref               :: reference(),
  status                  :: true | false, %% healthy or not
  connection_issues = []  :: list(),
  message_buffer = []     :: list()
}).
%%
%%  MQTT Topic structure:
%%  tgw/sys/faxe/{{device_id}}/health/{{flow_id}}
%%
%%  device_id : balena device-id or name of the faxe instance (faxe-1, faxe-2, faxe-3)
%%  flow_id : name of the faxe flow
%%
%%  {"status": 1} -> healthy no more info in the message, or do we need details ?
%%  {"status": 0} -> problems, 2 different fields could be added (for now):
%%
%%  possible problems:
%%  (not) running -> implicit, when health message is sent, that means that the flow is running
%%  connection problems
%%  processing errors
%%  no data seen / processed -> maybe better: last item seen/processed
%%
%%  "conn_status" - for any connection problems and
%%  "processing_errors" - for any errors that occur during processing of messages
%%  "errors" - for any
%%
%%  example:{"status": 0, "conn_status" : [conn_status problems], "processing_errors": [list errors]}
%%
%%  example processing error:
%%  {
%%  "node_id":"eval2",
%%  "meta":{
%%  "pid":"<0.1686.0>",
%%  "node":"faxe@127.0.0.1",
%%  "module":"df_component",
%%  "line":"{404,10}",
%%  "function":"handle_info",
%%  "device":"heyoka_local",
%%  %% "application":"faxe_common"
%%  },
%%  "message":"'error' in component esp_eval caught when processing item: {1,{data_point,1696934808000,#{<<\"a\">> => <<\"b\">>,<<\"c\">> => 4},#{},undefined,<<>>}} -- \"\\n    df_component:handle_info/2 line 402\\n    esp_eval:process/3 line 41\\n    esp_eval:eval/4 line 46\\n    lists:foldl/3 line 1350\\n    esp_eval:'-eval/4-fun-0-'/3 line 49\\n    faxe_lambda:execute/3 line 53\\n    lambda_12369952:lambda_12369952/1 line 3\\n    erlang:'*'(undefined, 2)\\nEXIT:badarith\"",
%%  "level":"error",
%%  "flow_id":"processing_errors"
%%  }
%%
%%  example conn_status:
%%
%%  {"port":102,"peer":"192.168.212.002","node_id":"s7read1","flow_id":"PLC_Counter_$ba9e4dd8-0059-e3ed-97b8-6feb3254e987.28938D","connected":false,"conn_type":"s7"}

%%%==================================================================
%%% API
%%%==================================================================

is_alive(Name) ->
  case get_observer(Name) of
    undefined -> false;
    Pid -> is_process_alive(Pid)
  end.

get_observer(FlowId) ->
  case ets:lookup(?ETS_FLOW_OBSERVER_TABLE, FlowId) of
    [] -> undefined;
    [{Name, Pid}] -> Pid
  end.

stop(ObserverPid) when is_pid(ObserverPid) ->
  gen_server:stop(ObserverPid);
stop(_) ->
  ok.
%%%===================================================================
%%% Spawning and gen_server implementation
%%%===================================================================

-spec start_link(FlowId :: binary()) -> any().
start_link(FlowId) ->
  gen_server:start_link(?MODULE, [FlowId, self()], []).

start_monitor(FlowId) ->
  case gen_server:start(?MODULE, [FlowId, self()], []) of
    {ok, Pid} -> erlang:monitor(process, Pid), {ok, Pid};
    Other -> Other
  end.


-spec init(list()) -> {ok, #state{}}.
init([FlowId, GraphPid]) ->
  erlang:monitor(process, GraphPid),
  ets:insert(?ETS_FLOW_OBSERVER_TABLE, {FlowId, self()}),

  %% MQTT
  MqttOpts = #{host := Host, port := Port} = mqtt_opts(),

  connection_registry:reg({<<"sys">>, <<"sys">>}, Host, Port, <<"mqtt">>),
%%  lager:warning("~p MQTT Opts: ~p",[?MODULE, MqttOpts]),
  %% use mqtt publisher pool !!
  mqtt_pub_pool_manager:connect(MqttOpts),

  %%% TOPIC
  DeviceName = faxe_util:device_name(),
  Topic = faxe_util:build_topic([?BASE_TOPIC, DeviceName, ?TOPIC_KEY, FlowId]),

  %%% REPORT TIMER
  Timer = start_timer(?REPORT_INTERVAL),

  {ok, #state{flow_id = FlowId, topic = Topic, host = Host, timer_ref = Timer, graph = GraphPid}}.

mqtt_opts() ->
  MqttOpts0 = faxe_config:get(mqtt, []),
  HandlerOptsAll = faxe_config:get(flow_health, []),
  HandlerOpts0 =
    case proplists:get_value(handler, HandlerOptsAll) of
      undefined -> [];
      List when is_list(List) ->
        %% _Type here is "mqtt" always at the moment, we do not care, could also be amqp
        %% next line probably not needed, enabling observer is done differently
        lists:filter(fun({_Type, E}) -> proplists:get_value(enable, E) end, List)
    end,
  HandlerOpts = proplists:get_value(mqtt, HandlerOpts0, []),
  MqttOpts2 = faxe_util:proplists_merge(faxe_config:filter_empty_options(HandlerOpts), MqttOpts0),
  MqttOpts3 = maps:from_list(MqttOpts2),
  MqttOpts3#{retained => false}.


handle_call(_Request, _From, State = #state{}) ->
  {reply, ok, State}.

handle_cast(_Request, State = #state{}) ->
  {noreply, State}.


handle_info({'DOWN', _Mon, process, Pid, Info}, State = #state{graph = Pid}) when Info == normal; Info == shutdown ->
  lager:notice("[~p] flow stopping normal, by request or timeout, will also stop now", [?MODULE]),
  NewState = publish(jiffy:encode(?MSG_STOPPED), State),
  {stop, normal, NewState};
handle_info({'DOWN', _Mon, process, Pid, Info}, State = #state{graph = Pid}) ->
  lager:notice("[~p] flow graph pid is DOWN for flow ~p with Reason ~p, will stop now",
    [?MODULE, State#state.flow_id, Info]),
  InfoMsg = list_to_binary(io_lib:format("~p", [Info])),
  Msg = ?MSG_CRASHED#{?FIELD_MESSAGE => InfoMsg},
  NewState = publish(jiffy:encode(Msg), State),
  {stop, normal, NewState};
%% no connection issues, status ok
handle_info(report, State = #state{connection_issues = []}) ->
  NewTimer = start_timer(?REPORT_INTERVAL),
  NewState = publish(jiffy:encode(?MSG_HEALTHY), State),
  {noreply, NewState#state{timer_ref = NewTimer}};
handle_info(report, State = #state{status = false, connection_issues = ConnIssues}) ->
  NewTimer = start_timer(?REPORT_INTERVAL),
  NewState = publish_conn_status(ConnIssues, State),
  {noreply, NewState#state{timer_ref = NewTimer}};
handle_info({mqtt_connected, _}, State) ->
  connection_registry:connected(),
  %% send buffered messages
  NewState = send_buffer(State#state{mqtt_connected = true}),
  {noreply, NewState};
handle_info({mqtt_disconnected, _}, State) ->
  connection_registry:disconnected(),
  {noreply, State#state{mqtt_connected = false}};
handle_info({log, Item}, State) ->
  NewState = cancel_timer(State),
%%  lager:info("~p got log item: ~p", [{?MODULE, self()}, Item]),
  Item0 = flowdata:from_json_struct(Item),
  Msg0 = flowdata:to_mapstruct(Item0),
  Msg = ?MSG_UNHEALTHY#{?FIELD_ERRORS => [Msg0]},
  NewState1 = publish(jiffy:encode(Msg), NewState),
  {noreply, NewState1#state{timer_ref = start_timer(?REPORT_INTERVAL)}};
handle_info({conn_status, Item}, State) ->
  NewState = cancel_timer(State),
%%  lager:notice("~p conn_status ~p",[{?MODULE, self()}, Item]),
  NewState1 = conn_status_received(NewState, Item),
  {noreply, NewState1#state{timer_ref = start_timer(?REPORT_INTERVAL)}};
handle_info(Info, State = #state{}) ->
  lager:warning("~p got unexpected message ~p",[?MODULE, Info]),
  {noreply, State}.

terminate(Reason, _State = #state{flow_id = FlowId}) ->
  lager:warning("~p terminates with reason: ~p",[?MODULE, Reason]),
  remove_flow(FlowId).

code_change(_OldVsn, State = #state{}, _Extra) ->
  {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
make_conn_ref(ConnStatus = #data_point{}) ->
  flowdata:fields(ConnStatus, ?CONN_REF_FIELDS).

%% ignore status "connecting"
conn_status_received(State, P = #data_point{fields = #{?FIELD_CONN_STATUS := 1}}) ->
  lager:info("ignore conn_status 'connecting' ~p", [lager:pr(P, ?MODULE)]),
  State;
conn_status_received(State = #state{connection_issues = []}, ConnStatus = #data_point{}) ->
  case flowdata:field(ConnStatus, ?CONN_FIELD_CONNECTED) of
    true ->
      publish(jiffy:encode(?MSG_HEALTHY), State);
    false -> <<"send the conn_status and add to issue list">>,
      NewIssueList = [{make_conn_ref(ConnStatus), ConnStatus}],
      NewState = publish_conn_status(NewIssueList, State),
      NewState#state{connection_issues = NewIssueList}
  end;
conn_status_received(State = #state{connection_issues = Issues}, ConnStatus = #data_point{}) ->
  ConnRef = make_conn_ref(ConnStatus),
  %% first see, if we have this status in our buffer
  Existing = proplists:get_value(ConnRef, Issues, undefined),
  NewConnIssues =
  case flowdata:field(ConnStatus, ?CONN_FIELD_CONNECTED) of
    true ->
      case Existing of
        undefined -> Issues;
        #data_point{} ->
          %% remove issue
          proplists:delete(ConnRef, Issues)
      end;
    false ->
      case Existing of
        undefined ->
          %% add to list
          [{ConnRef, ConnStatus}|Issues];
        #data_point{} ->
          %% already there, do nothing
          Issues
      end
  end,
  NewState =
  case NewConnIssues of
    [] ->
      publish(jiffy:encode(?MSG_HEALTHY), State);
    IssueList when is_list(IssueList) ->
      publish_conn_status(IssueList, State)
  end,
  NewState#state{connection_issues = NewConnIssues}.

-spec start_timer(Interval :: non_neg_integer()) -> reference().
start_timer(Interval) ->
  erlang:send_after(Interval, self(), report).

cancel_timer(State = #state{timer_ref = TimerRef}) ->
  catch erlang:cancel_timer(TimerRef),
  State#state{timer_ref = undefined}.

-spec remove_flow(Name :: binary()) -> true.
remove_flow(Name) ->
  ets:delete(?ETS_FLOW_OBSERVER_TABLE, Name).

send_buffer(State = #state{message_buffer = []}) ->
  State;
send_buffer(State = #state{message_buffer = Buffer}) ->
  [do_publish(Msg, State) || {_Ts, Msg} <- Buffer],
  State#state{message_buffer = []}.

-spec publish_conn_status(ConnStatusList :: list(), State::#state{}) -> #state{}.
publish_conn_status(ConnStatusList, State) ->
  MsgList = [maps:without([?FIELD_CONN_STATUS], flowdata:to_mapstruct(Item)) ||
    {_Ref, Item} <- ConnStatusList],
  Msg = ?MSG_UNHEALTHY#{?FIELD_CONN_STATUS => MsgList},
  publish(jiffy:encode(Msg), State).


-spec publish(Message :: any(), State :: #state{}) -> #state{}.
publish(Message, State=#state{mqtt_connected = false, message_buffer = Buf}) ->
  %% must add to buffer
  NewBuf = [{faxe_time:now(), Message} | Buf],
  State#state{message_buffer = NewBuf};
publish(Message, State) ->
  do_publish(Message, State),
  State.

do_publish(Message, #state{topic = Topic, host = Host}) ->
  {ok, Publisher} = mqtt_pub_pool_manager:get_connection(Host),
  Publisher ! {publish, {Topic, Message}}.

