%% Date: 27.03.2021 - 19:41
%% Ⓒ 2021 heyoka
%%
%%
-module(esp_group_by).
-author("Alexander Minichmair").

-include("faxe.hrl").

-behavior(df_component).
%% API
-export([init/3, process/3, options/0, handle_info/2]).

-record(state, {
   field,
   groups = #{},
   nodeid
}).

options() -> [
   {field, string}
].

init({_GId, NodeId}, _Inputs, #{field := Field}) ->
   {ok, all, #state{field = Field, nodeid = NodeId}}.

process(_In, P = #data_point{}, State = #state{field = FieldName, groups = Groups, nodeid = NId}) ->
   Value = flowdata:field(P, FieldName),
   NewGroups = ensure_route(Value, Groups, NId),
   OutPort = maps:get(Value, NewGroups),

   {emit, {OutPort, P}, State#state{groups = NewGroups}}.

handle_info({delete, Port}, State=#state{nodeid = NId}) ->
   df_graph:stop_subgraph(NId, Port),
   {ok, State}.

ensure_route(Val, Groups, _NId) when map_size(Groups) == 0 ->
%%   lager:notice("first call to ensure_route with: ~p",[Val]),
   Groups#{Val => 1};
ensure_route(Val, Groups, _NId) when is_map_key(Val, Groups) ->
   Groups;
%% @todo maybe we need a little trick here, cause the subscriptions for this node will be sent to it while we are
%% operation on this item, so the item will not be routable until the subscription update message will get to this node
%% which can happen only after we are done with the current item
%% (the same problem will arise, when removing a subgraph emanating from this node) <- should not be a problem
ensure_route(Val, Groups, NodeId) ->
%%   lager:warning("will have to start subgraph for grouping value: ~p",[Val]),
   NewPort = start_branch(NodeId),
   Groups#{Val => NewPort}.

start_branch(NodeId) ->
   {T, {ok, NewOutPort}} = timer:tc(df_graph, start_subgraph, [NodeId]),
   lager:alert("time to clone and start new subgraph: ~pms",[T]),
   erlang:send_after(10000, self(), {delete, NewOutPort}),
   NewOutPort.