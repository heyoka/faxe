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
   nodeid,

   group_last_seen = #{},
   reset_timeout,
   reset_check_interval = 30000
}).

options() -> [
   {field, string},
   %% not used yet:
   {reset_timeout, duration, <<"2m">>}
].

init({_GId, NodeId}, _Inputs, #{field := Field, reset_timeout := RTimeout}) ->
   ResetTimeout = faxe_time:duration_to_ms(RTimeout),
   State = #state{field = Field, nodeid = NodeId, reset_timeout = ResetTimeout},
   %% start reset interval
%%   erlang:send_after(State#state.reset_check_interval, self(), check_reset),
   {ok, all, State}.

process(_In, P = #data_point{},
    State = #state{field = FieldName, groups = Groups, nodeid = NId, group_last_seen = Last}) ->
%%   lager:info("##### groups are: ~p",[Groups]),
   Value = flowdata:field(P, FieldName),
   NewGroups = ensure_route(Value, Groups, NId),
   OutPort = maps:get(Value, NewGroups),

   {emit, {OutPort, P}, State#state{groups = NewGroups, group_last_seen = Last#{Value => faxe_time:now()}}}.

handle_info(check_reset, State=#state{reset_check_interval = Interval}) ->
   lager:info("check_reset timeout"),
   NewState = check_reset(State),
   erlang:send_after(Interval, self(), check_reset),
   {ok, NewState}.

ensure_route(Val, Groups, _NId) when map_size(Groups) == 0 ->
   Groups#{Val => 1};
ensure_route(Val, Groups, _NId) when is_map_key(Val, Groups) ->
   Groups;
ensure_route(Val, Groups, NodeId) ->
   NewPort = df_graph:start_subgraph(NodeId),
   Groups#{Val => NewPort}.


check_reset(State = #state{group_last_seen = LastSeen}) when map_size(LastSeen) == 0 ->
   lager:notice("check reset, nothing to do"),
   State;
check_reset(State = #state{group_last_seen = LastSeen, reset_timeout = Timeout, groups = Groups}) ->
   Now = faxe_time:now(),
   Fun =
      fun(Value, GroupLastSeen, Acc) ->
         case (GroupLastSeen + Timeout) < Now of
            true ->
               Port = maps:get(Value, Groups),
               [{Value, Port}|Acc];
            false -> Acc
         end
      end,
   ResetList = maps:fold(Fun, [], LastSeen),
   do_reset(ResetList, State).

do_reset([], State) ->
   State;
do_reset(ResetList, State = #state{group_last_seen = LastSeen, nodeid = NId, groups = Groups}) ->
   MapKeysResetted = proplists:get_keys(ResetList),
   lager:notice("reset: ~p, ~p",[ResetList, MapKeysResetted]),
   [df_graph:stop_subgraph(NId, Port) || {_Val, Port} <- ResetList],
   NewGroups = maps:without(MapKeysResetted, Groups),
   lager:notice("groups before reset: ~p",[Groups]),
   lager:notice("new groups after reset: ~p",[NewGroups]),
   State#state{groups = NewGroups, group_last_seen = maps:without(MapKeysResetted, LastSeen)}.