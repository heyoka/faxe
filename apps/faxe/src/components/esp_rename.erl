%% Date: 28.01.17 - 14:11
%% Ⓒ 2017 heyoka
%% @doc
%% Rename existing fields or tags, does not touch any values
%% Note: param-functions fields and as_fields AND tags and as_tags must have the same length
%% @end
-module(esp_rename).
-author("Alexander Minichmair").

%% API
-behavior(df_component).

-include("faxe.hrl").
%% API
-export([init/3, process/3, options/0
   , check_options/0
]).

-record(state, {
   node_id,
   fields,
   tags,
   as_fields,
   as_tags
}).

options() -> [
   {fields, binary_list, []},
   {as_fields, binary_list, []},
   {tags, binary_list, []},
   {as_tags, binary_list, []}].

check_options() ->
   [{same_length, [fields, as_fields]},
      {same_length, [tags, as_tags]}].

init(NodeId, _Ins, #{fields := Fields, tags := Tags, as_fields := AsFields, as_tags := AsTags}) ->
   {ok, all, #state{fields = Fields, node_id = NodeId, tags = Tags, as_fields = AsFields, as_tags = AsTags}}.

process(_In, #data_batch{points = Points} = Batch,
    State = #state{fields = FNames, as_fields = AsFields, tags = Tags, as_tags = AsTags}) ->
   NewPoints = lists:map(
      fun(P) ->
         NewPoint = flowdata:rename_fields(P, FNames, AsFields),
         flowdata:rename_tags(NewPoint, Tags, AsTags)
      end,
      Points
   ),
   {emit, Batch#data_batch{points = NewPoints}, State};

process(_Inport, #data_point{} = Point,
    State = #state{fields = FNames, as_fields = AsFields, tags = Tags, as_tags = AsTags}) ->

   NewPoint0 = flowdata:rename_fields(Point, FNames, AsFields),
   NewPoint = flowdata:rename_tags(NewPoint0, Tags, AsTags),
   {emit, NewPoint, State}.


