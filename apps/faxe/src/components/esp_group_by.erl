%% Date: 27.03.2021 - 19:41
%% Ⓒ 2021 heyoka
%%
%%
-module(esp_group_by).
-author("Alexander Minichmair").

-include("faxe.hrl").

-behavior(df_component).
%% API
-export([init/3, process/3, options/0]).

-record(state, {
   field,
   groups = #{},
   nodeid
}).

options() -> [
   {field, string}
].

init({_GId, NodeId}, _Inputs, #{field := Field}) ->
   lager:info("~p starting group_by: ~p",[NodeId, Field]),
   {ok, all, #state{field = Field, nodeid = NodeId}}.

process(_In, P = #data_point{}, State = #state{field = FieldName, groups = Groups, nodeid = NId}) ->
   Value = flowdata:field(P, FieldName),
   NewGroups = ensure_route(Value, Groups, NId),
   OutPort = maps:get(Value, NewGroups),
   {emit, {OutPort, P}, State#state{groups = NewGroups}}.

ensure_route(Val, Groups, _NId) when map_size(Groups) == 0 ->
   lager:notice("first call to ensure_route with: ~p",[Val]),
   Groups#{Val => 1};
ensure_route(Val, Groups, _NId) when is_map_key(Val, Groups) ->
   Groups;
ensure_route(Val, Groups, NodeId) ->
   lager:warning("will have to start subgraph for grouping value: ~p",[Val]),
   NewPort = start_branch(NodeId),
   Groups#{Val => NewPort}.

start_branch(NodeId) ->
   {ok, NewOutPort} = df_graph:start_subgraph(NodeId),
   NewOutPort.