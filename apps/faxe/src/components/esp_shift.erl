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

options() -> [{offset, binary, <<"30s">>}].

init(NodeId, _Ins, #{offset := Offset}=P) ->
   lager:info("Params for ~p ~p",[?MODULE, P]),
   {ok, all, #state{node_id = NodeId, offset = Offset}}.

process(_In, #data_batch{points = Points} = Batch, State = #state{offset = Offset}) ->
   lager:info("~p Ts before: ~p",[?MODULE, [faxe_time:to_date(T0) || T0 <- flowdata:ts(Batch)]]),
   NewPoints = [execute(Point,Offset) || Point <- Points],
   NewBatch = flowdata:set_bounds(Batch#data_batch{points = NewPoints}),
   lager:info("~p Ts after: ~p",[?MODULE, [faxe_time:to_date(T1) || T1 <- flowdata:ts(NewBatch)]]),
   {emit, NewBatch, State}
;
process(_Inport, #data_point{} = Point, State = #state{offset = Offset}) ->
   NewValue = execute(Point,Offset),
   lager:info("~p emitting: ~p :: ~p",[?MODULE, NewValue, faxe_time:to_date(flowdata:ts(NewValue))]),
   {emit, NewValue, State}.


execute(#data_point{ts = Ts} = Point, Offset) ->
   lager:debug("~p process, ~p :: ~p~n",[?MODULE, Point, faxe_time:to_date(flowdata:ts(Point))]),
   NewTs = faxe_time:add(Ts, Offset),
   Point#data_point{ts=NewTs}.