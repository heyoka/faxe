%% Date: 16.02.2020
%% Ⓒ 2020 heyoka
%% @doc
%% collect internal flow metrics data_points
%%
%%
%%
-module(esp_metrics).
-author("Alexander Minichmair").

-behaviour(df_component).

-include("faxe.hrl").

%% API
-export([init/3, process/3, handle_info/2, options/0]).

-record(state, {
   flow_id,
   node_id,
   metrics
}).

options() ->
   [
      {flow, string},
      {node, string, undefined},
      {metrics, string_list, undefined}
   ].

init({FId, _NId}, _Inputs, #{flow := FlowId, metrics := Metrics, node := NodeId}) ->
   case FlowId == FId of
      true -> lager:error("can not use 'metrics' node within source flow.");
      false ->
         gen_event:add_sup_handler(faxe_metrics, metrics_handler_dataflow,
            #{parent => self(), flow_id => FlowId, node_id => NodeId, metrics => Metrics})
   end,
   State = #state{flow_id = FlowId, metrics = Metrics, node_id = NodeId},
   {ok, all, State}.

process(_, _Item, State=#state{} ) ->
   {ok, State}.

handle_info({_, Point}, State = #state{}) ->
%%   lager:notice("got datapoint: ~p",[Point]),
   {emit, {1, Point}, State};
handle_info(_Request, State) ->
   {ok, State}.


%%%===================================================================
%%% Internal functions
%%%===================================================================


