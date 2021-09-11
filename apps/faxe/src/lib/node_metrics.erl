%%%-------------------------------------------------------------------
%%% @author heyoka
%%% @copyright (C) 2020, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 28. May 2020 15:38
%%%-------------------------------------------------------------------
-module(node_metrics).
-author("heyoka").
-include("faxe.hrl").
%% API
-export([
  setup/3,
  destroy/3,

  setup_node_metrics/2,
  setup_node_metrics/3,
  destroy_node_metrics/3,

  process_metrics/2,

  metric/3,
  metric/4,
  metric_name/3,
  node_metrics/1]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% METRICS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% setup all metrics for a flow
setup(FlowId, NodeId, Component) ->
  Ms = node_metrics(Component),
  setup_node_metrics(FlowId, NodeId, Ms).

destroy(FlowId, NodeId, Component) ->
  Common = ?NODE_COMMON_METRICS,
  AddMetrics = get_node_metrics(Component),
  destroy_node_metrics(FlowId, NodeId, Common ++ AddMetrics).

node_metrics(Component) ->
  Common = ?NODE_COMMON_METRICS,
  AddMetrics = get_node_metrics(Component),
  lager:notice("component for metrics: ~p ::: ~p" ,[Component, AddMetrics]),
  Common ++ AddMetrics.
  %,
  %lager:notice("nodemetrics: ~p : ~p",[{FlowId, NodeId, Component}, Ms]),

%%% @doc get extra node metrics from a component module
get_node_metrics(Component) ->
  case erlang:function_exported(Component, metrics, 0) of
    true -> maybe_add_bytes_metrics(Component:metrics());
    false -> []
  end.

maybe_add_bytes_metrics(ComponentMetrics) ->
  lager:notice("maybe_add_bytes_metrics: ~p", [ComponentMetrics]),
  M0 =
  case lists:keyfind(?METRIC_BYTES_READ, 1, ComponentMetrics) of
    T when is_tuple(T) -> ComponentMetrics ++ [{?METRIC_BYTES_READ_SIZE, histogram, []}];
    false -> ComponentMetrics
  end,
  lager:notice("M0 :: ~p",[M0]),
  case lists:keyfind(?METRIC_BYTES_SENT, 1, M0) of
    T1 when is_tuple(T1) -> M0 ++ [{?METRIC_BYTES_SENT_SIZE, histogram, []}];
    false -> M0
  end.


setup_node_metrics(FlowId, NodeId, MetricList) when is_list(MetricList) ->
  NId = id(FlowId, NodeId),
  setup_node_metrics(NId, MetricList).
setup_node_metrics(FlowNodeId, MetricList) when is_binary(FlowNodeId) ->
  MF =
    fun
      ({MetricName, MetricType, Opts}) ->
        Name0 = name(FlowNodeId, MetricName),
        new(MetricType, Name0, Opts),
        folsom_metrics:tag_metric(Name0, MetricName);
      ({MetricName, MetricType, Opts, _Desc}) ->
        Name1 = name(FlowNodeId, MetricName),
        new(MetricType, Name1, Opts),
        folsom_metrics:tag_metric(Name1, MetricName)
    end,
  lists:foreach(MF, MetricList).

destroy_node_metrics(FlowId, NodeId, MetricList) ->
  NId = id(FlowId, NodeId),
  MF = fun({MetricName, _MetricType, _, _}) -> folsom_metrics:delete_metric(<<NId/binary, MetricName/binary>>) end,
  lists:foreach(MF, MetricList).

%%% @doc update a metric
metric(Name, Value, FlowId, NodeId) ->
  metric(id(FlowId, NodeId), Name, Value).
metric(Name, Value, {FlowId, NodeId}) ->
  metric(Name, Value, FlowId, NodeId);
metric(FlowNodeId, ?METRIC_BYTES_READ, Value) when is_binary(FlowNodeId) ->
  %% add Value to histogram to have average value ready
  folsom_metrics:notify({<<FlowNodeId/binary, ?METRIC_BYTES_READ_SIZE/binary>>, Value}),
  folsom_metrics:notify({<<FlowNodeId/binary, ?METRIC_BYTES_READ/binary>>, Value});
metric(FlowNodeId, ?METRIC_BYTES_SENT, Value) when is_binary(FlowNodeId) ->
  %% add Value to histogram to have average value ready
  folsom_metrics:notify({<<FlowNodeId/binary, ?METRIC_BYTES_SENT_SIZE/binary>>, Value}),
  folsom_metrics:notify({<<FlowNodeId/binary, ?METRIC_BYTES_SENT/binary>>, Value});
metric(FlowNodeId, Name, Value) when is_binary(FlowNodeId), is_binary(Name) ->
  folsom_metrics:notify({<<FlowNodeId/binary, Name/binary>>, Value}).

%%% @doc get process_info metrics and send them to the metrics backend
process_metrics(FlowId, {NId, _C, NPid}) ->
  {message_queue_len, MsgQueueLength} = erlang:process_info(NPid, message_queue_len),
  {memory, MemUsage} = erlang:process_info(NPid, memory),

  metric(?METRIC_MSG_Q_SIZE, MsgQueueLength, FlowId, NId),
  metric(?METRIC_MEM_USED, MemUsage, FlowId, NId).

%%% @doc setup metrics
new(meter, Name, _) ->
  folsom_metrics:new_meter(Name);
new(gauge, Name, _) ->
  folsom_metrics:new_gauge(Name);
new(counter, Name, _) ->
  folsom_metrics:new_counter(Name);
new(spiral, Name, _) ->
  folsom_metrics:new_spiral(Name);
new(histogram, Name, []) ->
  new(histogram, Name, [slide, 60]);
new(histogram, Name, [Type, WinSize]) ->
  folsom_metrics:new_histogram(Name, Type, WinSize).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% @doc get a metric name
name(FlowNodeId, MetricName) ->
  <<FlowNodeId/binary, MetricName/binary>>.

%% @doc get a flownodeid identifing a node
id(FlowId, NodeId) ->
  <<FlowId/binary, "==", NodeId/binary>>.

metric_name(FlowId, NodeId, MetricName) ->
  Id = id(FlowId, NodeId),
  name(Id, MetricName).