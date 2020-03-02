%% Date: 28.01.17 - 14:11
%% Ⓒ 2017 heyoka
%% @doc
%% Set fields and/or tags to data_point or data_batch items
%% @end
-module(esp_set).
-author("Alexander Minichmair").

%% API
-behavior(df_component).

-include("faxe.hrl").
%% API
-export([init/3, process/3, options/0, check_options/0]).

-record(state, {
   node_id,
   fields,
   tags,
   field_values,
   tag_values,
   field_kvs,
   tag_kvs
}).

options() -> [
   {fields, binary_list, []},
   {field_values, list, []},
   {tags, binary_list, []},
   {tag_values, binary_list, []}].

check_options() ->
   [
      {same_length, [fields, field_values]}, {same_length, [tags, tag_values]}
   ].

init(NodeId, _Ins, #{fields := Fields0, tags := Tags0,
   tag_values := TagV, field_values := FieldV}) ->
   %% optimize field_lookup by translating the possibly deep path to its tuple form
   Fields = [flowdata:path(F) || F <- Fields0],
   Tags = [flowdata:path(T) || T <- Tags0],
   {ok, all,
      #state{field_kvs = lists:zip(Fields, FieldV), tag_kvs = lists:zip(Tags, TagV),
         fields = Fields, node_id = NodeId, tags = Tags,
         tag_values = TagV, field_values = FieldV}}.

process(_In, Item, State = #state{field_kvs = Fields,  tag_kvs = Tags}) ->
   NewItem0 = flowdata:set_fields(Item, Fields),
   NewItem = flowdata:set_tags(NewItem0, Tags),
   {emit, NewItem, State}.

