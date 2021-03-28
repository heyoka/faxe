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

init(NodeId, _Inputs, #{field := Field}) ->
   {ok, all, #state{field = Field, nodeid = NodeId}}.

process(_In, P = #data_point{}, State = #state{field = FieldName, groups = Groups}) ->
   Value = flowdata:field(P, FieldName),
   NewGroups = ensure_route(Value, Groups),
   OutPort = maps:get(Value, NewGroups),
   {emit, {OutPort, P}, State#state{groups = NewGroups}}.

ensure_route(Val, Groups) when map_size(Groups) == 0 ->
   Groups#{Val => 1};
ensure_route(Val, Groups) when is_map_key(Val, Groups) ->
   Groups;
ensure_route(Val, Groups) ->
   NewPort = start_branch(),
   Groups#{Val => NewPort}.

start_branch() ->
   NewOutPort = df_graph:clone_and_start_subgraph(self()),
   NewOutPort.