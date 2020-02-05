%% Date: 28.01.17 - 14:11
%% Ⓒ 2017 heyoka
%% @doc
%% Delete value(s) with the given fieldname or path from data_point and data_batch
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

process(_Inport, Item, State = #state{fields = Fs, tags = Ts}) ->
   NewPoint = do_delete(Fs, Ts, Item),
   {emit, NewPoint, State}.

do_delete(Fields, [], Item) ->
   flowdata:delete_fields(Item, Fields);
do_delete([], Tags, Item) ->
   flowdata:delete_tags(Item, Tags);
do_delete(Fields, Tags, Item) ->
   P = flowdata:delete_fields(Item, Fields),
   flowdata:delete_tags(P, Tags).

%%-spec delete(list(), list(), #data_point{}) -> #data_point{}.
%%delete([],[],Point=#data_point{}) ->
%%   Point;
%%delete([F|Fields], [], Point=#data_point{}) ->
%%   delete(Fields, [], flowdata:delete_field(Point, F));
%%delete([],[T|Tags], Point=#data_point{}) ->
%%   delete([], Tags, flowdata:delete_tag(Point, T));
%%delete([F|Fields],[T|Tags], Point=#data_point{}) ->
%%   P0 = flowdata:delete_field(Point, F),
%%   delete(Fields, Tags, flowdata:delete_tag(P0, T)).


