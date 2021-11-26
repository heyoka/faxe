%% Date: 28.01.17 - 14:11
%% Ⓒ 2017 heyoka
%% @doc
%% Delete value(s) with the given fieldname or path from data_point and data_batch
%%
%% Now, conditional delete with a lambda expression is possible.
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
   tags,
   condition
}).

options() -> [
   {fields, binary_list, []},
   {tags, binary_list, []},
   {where, lambda, undefined} %% for conditional delete
].

init(NodeId, _Ins, #{fields := Fields, tags := Tags, where := Condition}) ->
   {ok, all, #state{fields = Fields, node_id = NodeId, tags = Tags, condition = Condition}}.

process(_In, Item, State = #state{condition = undefined, fields = Fs, tags = Ts}) ->
   NewItem = do_delete(Fs, Ts, Item),
   {emit, NewItem, State};
process(_In, Batch = #data_batch{points = Points}, State) ->
   NewPoints = [process_point(P, State) || P <- Points ],
   {emit, Batch#data_batch{points = NewPoints}, State};
process(_Inport, Item = #data_point{}, State = #state{}) ->
   NewPoint = process_point(Item, State),
   {emit, NewPoint, State}.

process_point(Point, #state{fields = Fs, tags = Ts, condition = Cond}) ->
   case eval(Point, Cond) of
      true ->
%%         lager:info("delete : ~p from :~p",[Fs, Point]),
         do_delete(Fs, Ts, Point);
      false -> Point
   end.

do_delete(Fields, [], Item) ->
   flowdata:delete_fields(Item, Fields);
do_delete([], Tags, Item) ->
   flowdata:delete_tags(Item, Tags);
do_delete(Fields, Tags, Item) ->
   P = flowdata:delete_fields(Item, Fields),
   flowdata:delete_tags(P, Tags).

eval(_Point, undefined) -> true;
eval(Point, Fun) when is_function(Fun) -> faxe_lambda:execute_bool(Point, Fun).

