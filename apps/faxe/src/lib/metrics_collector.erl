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

-record(metrics_collector_state, {}).

-define(INTERVAL, 20000).
%%%===================================================================
%%% Spawning and gen_server implementation
%%%===================================================================

start_link() ->
  gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

init([]) ->
  erlang:send_after(?INTERVAL, self(), collect),
  {ok, #metrics_collector_state{}}.

handle_call(_Request, _From, State = #metrics_collector_state{}) ->
  {reply, ok, State}.

handle_cast(_Request, State = #metrics_collector_state{}) ->
  {noreply, State}.

handle_info(collect, State = #metrics_collector_state{}) ->
  {TimeToCollect, All} = timer:tc(?MODULE, do_collect, []),
%%  Ms = metrics(),
  lager:info("Metrics collection took: ~p my",[TimeToCollect]),
%%  All = [collect(M) || M <- Ms],
  lists:foreach(fun({_FlowId, NMS}) ->
%%    {faxe_metrics, flow}
    [gen_event:notify(faxe_metrics, {node, Met}) || Met <- NMS]
%%    {faxe_metrics, node, }
%%    [lager:notice("~p",[Met]) ]
    end,
    All),

%%  Vals = folsom_metrics:get_metrics_value(?METRIC_ITEMS_PROCESSED),
%%  Errs = folsom_metrics:get_metrics_value(?METRIC_ERRORS),
%%  NId = <<"change_detect2">>,
%%  NId = <<"s7read_pool1">>,
%%  ItemsIn = folsom_metrics:get_metric_value(<<NId/binary, ?METRIC_ITEMS_IN/binary>>),
%%  ItemsOut = folsom_metrics:get_metric_value(<<NId/binary, ?METRIC_ITEMS_OUT/binary>>),
%%%%  ProcTime = folsom_metrics:get_metric_value(<<NId/binary, ?METRIC_PROCESSING_TIME/binary>>),
%%  ProcTime = folsom_metrics:get_histogram_statistics(<<NId/binary, ?METRIC_PROCESSING_TIME/binary>>),
%%%%  ReadTime = folsom_metrics:get_histogram_statistics(<<NId/binary, ?METRIC_READING_TIME/binary>>),
%%%%  BytesRead = folsom_metrics:get_histogram_statistics(<<NId/binary, ?METRIC_BYTES_READ/binary>>),
%%  MemUsed = folsom_metrics:get_histogram_statistics(<<NId/binary, ?METRIC_MEM_USED/binary>>),
%%  QSize = folsom_metrics:get_histogram_statistics(<<NId/binary, ?METRIC_MSG_Q_SIZE/binary>>),
%%%%  lager:warning("Processing Time: ~p",[ProcTime]),
%%  lager:warning("Message Q Size: ~p",[QSize]),
%%  lager:warning("Memory consumption: ~p",[MemUsed]),
%%%%  lager:warning("Bytes Read: ~p",[BytesRead]),
%%%%  lager:warning("Reading Time: ~p",[ReadTime]),
%%  lager:warning("Items In: ~p",[ItemsIn]),
%%  lager:warning("Items Out: ~p" ,[ItemsOut]),
%%  [lager:warning("items_processed: ~p",[{Name, Val}]) || {Name, Val} <- Vals],
%%  [lager:warning("errors: ~p",[{Name, Val}]) || {Name, Val} <- Errs],
  erlang:send_after(?INTERVAL, self(), collect),
  {noreply, State};
handle_info(_Info, State = #metrics_collector_state{}) ->
  {noreply, State}.

terminate(_Reason, _State = #metrics_collector_state{}) ->
  ok.

code_change(_OldVsn, State = #metrics_collector_state{}, _Extra) ->
  {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
do_collect() ->
  Ms = metrics(),
  All = [collect(M) || M <- Ms],
  All.

get_flows() ->
  faxe:list_running_tasks()++faxe:list_temporary_tasks().

flow_nodes(FlowList) ->
  F = fun(E) ->
          {FlowId, GPid} =
          case E of
            #task{name = FlowIdx, pid = Pid} ->
              {FlowIdx, Pid};
            {_FlowId, _Pid} = I ->
              I
          end,
          Nodes = df_graph:nodes(GPid),
          {FlowId, Nodes}
      end,
  lists:map(F, FlowList).

metrics() ->
  FlowNodes = flow_nodes(get_flows()),
%%  lager:notice("FlowNodes: ~p",[FlowNodes]),
  metrics(FlowNodes, []).

metrics([], Acc) ->
  Acc;
metrics([{FlowId, Nodes}| R]=L, Acc) ->
%%  lager:notice("metrics_names: ~p, ~p",[L,  Acc]),
  F = fun({NId, Comp, _Pid}) ->
    AllNodeMetrics = node_metrics:node_metrics(Comp),
    [#{
      name => node_metrics:metric_name(FlowId, NId, MetricName),
      %flow_id => FlowId,
      node_id => NId,
      metric_name => MetricName,
      type => MetricType
    }

      || {MetricName, MetricType, _, _ } <- AllNodeMetrics
    ]
      end,
  metrics(R, Acc ++ [{FlowId, lists:flatmap(F, Nodes)}]).

collect({FId, Metrics}) ->
  {FId, collect(Metrics, [])}.
collect([], Acc) ->
  Acc;
collect([#{type := Type, name := Name} = M|R], Acc) ->
  collect(R, [M#{data => get_data(Type, Name)}|Acc]).

get_data(histogram, Name) ->
  S = folsom_metrics:get_histogram_statistics(Name),
  maps:to_list(maps:with([arithmetic_mean, min, max, n], maps:from_list(S)));
get_data(meter, Name) ->
  S = folsom_metrics:get_metric_value(Name),
  maps:to_list(maps:with([count, instant, one, five, fifteen], maps:from_list(S)));
get_data(_, Name) ->
  D = folsom_metrics:get_metric_value(Name),
  D.


