%% Date: 16.02.2020
%% Ⓒ 2020 heyoka
%% @doc
%% collect internal flow metrics data_points
%%
%%
%%
-module(esp_conn_status).
-author("Alexander Minichmair").

-behaviour(df_component).

-include("faxe.hrl").

%% API
-export([init/3, process/3, handle_info/2, options/0]).

-record(state, {
   flow_id,
   node_id,
   conn_type
}).

options() ->
   [
      {flow, string},
      {node, string, undefined},
      {type, string, undefined}
   ].

init({FId, _NId}, _Inputs, #{flow := FlowId, node := NodeId, type := CType}) ->
   case FlowId == FId of
      true -> lager:error("can not use 'metrics' node within source flow.");
      false ->
         gen_event:add_sup_handler(conn_status, conn_status_handler_dataflow,
            #{parent => self(), flow_id => FlowId, node_id => NodeId})
   end,
   State = #state{flow_id = FlowId, node_id = NodeId, conn_type = CType},
   {ok, all, State}.

process(_, _Item, State=#state{} ) ->
   {ok, State}.

handle_info({_, Point}, State = #state{conn_type = undefined}) ->
   {emit, {1, Point}, State};
handle_info({_, Point}, State = #state{conn_type = Type}) ->
   lager:notice("got datapoint: ~p :: conn_type: ~p",[Point, flowdata:field(Point, <<"conn_type">>)]),
   case flowdata:field(Point, <<"conn_type">>) == Type of
      true -> {emit, {1, Point}, State};
      false -> {ok, State}
   end;
handle_info(_Request, State) ->
   {ok, State}.


%%%===================================================================
%%% Internal functions
%%%===================================================================


