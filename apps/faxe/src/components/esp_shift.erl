%% Date: 05.01.17 - 14:11
%% Ⓒ 2017 heyoka
%%
%% @doc
%% Shift Timestamp of value(s) by unit and interval ie duration
%% @end
-module(esp_shift).
-author("Alexander Minichmair").

%% API
-behavior(df_component).

-include("faxe.hrl").
%% API
-export([init/3, process/3, options/0]).

-record(state, {
   node_id,
   offset
}).

options() -> [{offset, duration, <<"-30s">>}].

init(NodeId, _Ins, #{offset := Offset}) ->
   {ok, all, #state{node_id = NodeId, offset = Offset}}.

process(_In, #data_batch{points = Points} = Batch, State = #state{offset = Offset}) ->
   NewPoints = [execute(Point,Offset) || Point <- Points],
   NewBatch = flowdata:set_bounds(Batch#data_batch{points = NewPoints}),
   {emit, NewBatch, State}
;
process(_Inport, #data_point{} = Point, State = #state{offset = Offset}) ->
   NewValue = execute(Point,Offset),
   {emit, NewValue, State}.


-spec execute(#data_point{}, binary()) -> #data_point{}.
execute(#data_point{ts = Ts} = Point, Offset) ->
   NewTs = faxe_time:add(Ts, Offset),
   flowdata:set_ts(Point, NewTs).