%%%-------------------------------------------------------------------
%%% @author heyoka
%%% @copyright (C) 2020, <COMPANY>
%%% @doc
%%% @end
%%%-------------------------------------------------------------------
-module(metrics_collector).

-behaviour(gen_server).

-export([start_link/0, metrics/0, do_collect/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
  code_change/3]).

-include("faxe.hrl").
-define(SERVER, ?MODULE).

-record(metrics_collector_state, {timer}).

-define(INTERVAL, <<"30s">>).

%%%===================================================================
%%% Spawning and gen_server implementation
%%%===================================================================

start_link() ->
  gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

init([]) ->
  Interval = faxe_util:to_bin(faxe_config:get_sub(metrics, publish_interval, ?INTERVAL)),
  Timer = faxe_time:init_timer(true, Interval, collect),
  {ok, #metrics_collector_state{timer = Timer}}.

handle_call(_Request, _From, State = #metrics_collector_state{}) ->
  {reply, ok, State}.

handle_cast(_Request, State = #metrics_collector_state{}) ->
  {noreply, State}.

handle_info(collect, State = #metrics_collector_state{timer = Timer}) ->
  All = do_collect(),
  Ts = Timer#faxe_timer.last_time,
%%  {TimeToCollect, All} = timer:tc(?MODULE, do_collect, []),
%%  lager:info("Metrics collection took: ~p my",[TimeToCollect]),
  lists:foreach(
    fun({FlowId, NMS} = DP) ->
%%      lager:info("FlowId: ~p :: ~p",[FlowId, NMS]),
      publish(DP, Ts),
      publish_flow_metrics(FlowId, NMS, Ts)
%%      lager:notice("FlowMetrics: ~p",[publish_flow_metrics(FlowId, NMS)])
    end,
    All),

%%  erlang:send_after(?INTERVAL, self(), collect),
  {noreply, State#metrics_collector_state{timer = faxe_time:timer_next(Timer)}};
handle_info(_Info, State = #metrics_collector_state{}) ->
  {noreply, State}.

terminate(_Reason, _State = #metrics_collector_state{}) ->
  ok.

code_change(_OldVsn, State = #metrics_collector_state{}, _Extra) ->
  {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

publish({FlowId, Metrics}, Ts) ->
  F = fun(#data_point{fields = Fields} = P) ->
    NodeId = maps:get(<<"node_id">>, Fields),
    MetricName = maps:get(<<"metric_name">>, Fields),
%%    lager:notice("event: ~p ~n~s",[{FlowId, NodeId, MetricName}, flowdata:to_json(P#data_point{ts = Ts})]),
    gen_event:notify(faxe_metrics, {{FlowId, NodeId, MetricName}, P#data_point{ts = Ts}})
      end,
  lists:foreach(F, Metrics).

do_collect() ->
  Ms = metrics(),
  plists:map(fun(FlowMetrics) -> collect(FlowMetrics) end, Ms, 5).
%%  All = [collect(M) || M <- Ms],
%%  All.

get_flows() ->
  case (catch faxe:list_running_tasks() ++ faxe:list_temporary_tasks()) of
    List when is_list(List) -> List;
    _ -> []
  end.

flow_nodes(FlowList) ->
  F = fun(E, Acc) ->
    {FlowId, GPid} =
      case E of
        #task{name = FlowIdx, pid = Pid} ->
          {FlowIdx, Pid};
        {_FlowId, _Pid} = I ->
          I
      end,
    %% safety first here
    case is_process_alive(GPid) of
      true ->
        case catch df_graph:nodes(GPid) of
          Nodes when is_list(Nodes) ->
            [{FlowId, Nodes} | Acc];
          _ -> Acc
        end;
      false -> Acc
    end
      end,
  lists:foldl(F, [], FlowList).

metrics() ->
  FlowNodes = flow_nodes(get_flows()),
%%  lager:notice("metrics for : ~p",[FlowNodes]),
  metrics(FlowNodes, []).

metrics([], Acc) ->
  Acc;
metrics([{FlowId, Nodes}| R] = _L, Acc) ->
%%  lager:notice("metrics_names: ~p, ~p",[L,  Acc]),
  F = fun({NId, Comp, _Pid}) ->
    AllNodeMetrics = node_metrics:node_metrics(Comp),
%%    lager:info("AllNodeMetrics for ~p :: ~n",[Comp]),
%%    [lager:info("~p",[metric(M)]) || M <- AllNodeMetrics],
    lists:map(
      fun(RawMetric) ->
        Metric = #{metric_name := MetricName} = metric(RawMetric),
        Metric#{name => node_metrics:metric_name(FlowId, NId, MetricName),
          flow_id => FlowId,
          node_id => NId}
      end,
      AllNodeMetrics
    )
      end,
  metrics(R, Acc ++ [{FlowId, lists:flatmap(F, Nodes)}]).

metric({MetricName, MetricType, _, _ }) ->
  #{metric_name => MetricName, type => MetricType};
metric({MetricName, MetricType, _}) ->
  #{metric_name => MetricName, type => MetricType};
metric({MetricName, MetricType}) ->
  #{metric_name => MetricName, type => MetricType}.

publish_flow_metrics(FlowId, Metrics, Ts) ->
  F =
  fun(#data_point{fields = #{<<"metric_name">> := MName}=Fields}=_P, Acc) ->
    Val =
    case MName of
      ?METRIC_ERRORS -> maps:get(<<"counter">>, Fields);
      ?METRIC_ITEMS_IN -> maps:get(<<"total">>, Fields);
      ?METRIC_ITEMS_OUT -> maps:get(<<"total">>, Fields);
      ?METRIC_BYTES_READ -> maps:get(<<"five">>, Fields); %% bytes per second
      ?METRIC_BYTES_READ_SIZE -> maps:get(<<"mean">>, Fields); %% bytes average
      ?METRIC_BYTES_SENT -> maps:get(<<"five">>, Fields); %% bytes per second
      ?METRIC_BYTES_SENT_SIZE -> maps:get(<<"mean">>, Fields); %% bytes average
      ?METRIC_MSG_Q_SIZE -> maps:get(<<"gauge">>, Fields);
      ?METRIC_MEM_USED -> maps:get(<<"gauge">>, Fields);
      ?METRIC_PROCESSING_TIME -> maps:get(<<"mean">>, Fields);
      _ -> 0
    end,
    case maps:is_key(MName, Acc) of
      true -> Acc#{MName => [Val|maps:get(MName, Acc)]};
      false -> Acc#{MName => [Val]}
    end
  end,
  Grouped = lists:foldl(F, #{}, Metrics),
  FL =
  fun(K, V) ->
    case K of
      ?METRIC_ITEMS_IN -> lists:max(V); %% max of all nodes
      ?METRIC_ITEMS_OUT -> lists:max(V); %% max of all nodes
      ?METRIC_PROCESSING_TIME -> faxe_lambda_lib:round_float(lists:sum(V)/length(V), 3); %% average proc time
      ?METRIC_BYTES_SENT -> faxe_lambda_lib:round_float(lists:sum(V), 2); %% average bytes-sent per second
      ?METRIC_BYTES_READ -> faxe_lambda_lib:round_float(lists:sum(V), 2); %% average bytes-read per second
      ?METRIC_MEM_USED -> lists:sum(V); %%
      _ -> lists:sum(V)
    end
  end,
  FlowFields = maps:map(FL,  Grouped),
  P = #data_point{ts = Ts,
    fields = maps:merge(rename_metrics(FlowFields), #{<<"flow_id">> => FlowId})},
%%  lager:warning("FlowMETRICS: ~s" ,[flowdata:to_json(P)]),
  gen_event:notify(faxe_metrics, {{FlowId}, P}).

rename_metrics(Map = #{?METRIC_BYTES_SENT_SIZE := SentSize}) ->
  Map0 = maps:without([?METRIC_BYTES_SENT_SIZE], Map),
  rename_metrics(Map0#{<<"bytes_sent_avg">> => SentSize});
rename_metrics(Map= #{?METRIC_BYTES_READ_SIZE := ReadSize}) ->
  Map0 = maps:without([?METRIC_BYTES_READ_SIZE], Map),
  rename_metrics(Map0#{<<"bytes_read_avg">> => ReadSize});
rename_metrics(M) ->
  M.

collect({FId, Metrics}) ->
  {FId, collect(Metrics, [])}.
collect([], Acc) ->
%%  lager:notice("collected: ~p", [Acc]),
%%  build(Acc);
  Acc;
collect([#{type := Type, name := Name, node_id := NId, flow_id := FId, metric_name := MName} = _M|R], Acc) ->
  MTrans =
  #{
    <<"type">> => atom_to_binary(Type, latin1), <<"node_id">> => NId,
    <<"flow_id">> => FId, <<"metric_name">> => MName
  },
  Data0 =
  case (catch get_data(Type, Name)) of
    M when is_map(M) -> M;
    _ -> #{}
  end,
  Data = maps:merge(MTrans, Data0),
%%  lager:warning("collected: ~p", [Data]),
  collect(R, [#data_point{ts = faxe_time:now(), fields = Data} | Acc]).


get_data(histogram, Name) ->
  S = folsom_metrics:get_histogram_statistics(Name),
  M = maps:from_list(S),
%%  lager:notice("histogram metrics: ~p",[M]),
  #{
    <<"mean">> => faxe_lambda_lib:round_float(maps:get(arithmetic_mean, M), 3),
    <<"min">> => maps:get(min, M),
    <<"max">> => maps:get(max, M),
    <<"n">> => maps:get(n, M)
  };
get_data(meter, Name) ->
  S = folsom_metrics:get_metric_value(Name),
%%  lager:info("meter info: ~p", [folsom_metrics:get_metric_info(Name)]),
  M = maps:from_list(S),
%%  lager:notice("meter metrics: ~p",[M]),
  #{
    <<"total">> => maps:get(count, M),
    <<"instant">> => maps:get(instant, M),
    <<"one">> => maps:get(one, M),
    <<"five">> => maps:get(five, M),
    <<"fifteen">> => maps:get(fifteen, M)
%%    ,
%%    <<"mean">> => maps:get(mean, M)
  };
get_data(Type, Name) ->
  D = folsom_metrics:get_metric_value(Name),
  #{atom_to_binary(Type, latin1) => D}.


