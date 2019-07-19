%% Date: 28.01.17 - 14:11
%% Ⓒ 2017 heyoka
%% @doc
%% Delete Fields and Tags from datapoint and databatch
%% @end
-module(esp_delete).
-author("Alexander Minichmair").

%% API
-behavior(df_component).

-include("faxe.hrl").
%% API
-export([init/3, process/3, options/0]).

-record(state, {
   node_id,
   fields,
   tags
}).

options() -> [{fields, binary_list, []},{tags, binary_list, []}].

init(NodeId, _Ins, #{fields := Fields, tags := Tags}) ->
   {ok, all, #state{fields = Fields, node_id = NodeId, tags = Tags}}.

process(_In, #data_batch{points = Points} = Batch, State = #state{fields = Fs, tags = Ts}) ->
   NewPoints = lists:map(
      fun(P) -> delete(Fs, Ts, P) end,
      Points
   ),
   {emit, Batch#data_batch{points = NewPoints}, State};
process(_Inport, #data_point{} = Point, State = #state{fields = Fs, tags = Ts}) ->
   NewPoint = delete(Fs, Ts, Point),
   lager:info("~p emitting: ~p",[?MODULE, NewPoint]),
   {emit, NewPoint, State}.


-spec delete(list(), list(), #data_point{}) -> #data_point{}.
delete([],[],Point=#data_point{}) ->
   Point;
delete([F|Fields], [], Point=#data_point{}) ->
   delete(Fields, [], flowdata:delete_field(Point, F));
delete([],[T|Tags], Point=#data_point{}) ->
   delete([], Tags, flowdata:delete_tag(Point, T));
delete([F|Fields],[T|Tags], Point=#data_point{}) ->
   P0 = flowdata:delete_field(Point, F),
   delete(Fields, Tags, flowdata:delete_tag(P0, T)).


