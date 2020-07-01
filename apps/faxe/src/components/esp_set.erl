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
   tag_kvs,
   fields_root = false,
   tags_root = false
}).

options() -> [
   {fields, binary_list, []},
   {field_values, list, []},
   {tags, binary_list, []},
   {tag_values, binary_list, []}].

check_options() ->
   [
      {oneplus_of_params, [fields, tags]},
      {same_length, [fields, field_values]}, {same_length, [tags, tag_values]}
   ].

init(NodeId, _Ins, #{fields := Fields0, tags := Tags0,
   tag_values := TagV, field_values := FieldV}) ->
   %% optimize field_lookup by translating the possibly deep path to its tuple form and if all paths are
   %% just root paths, we work directly on the underlying map structure
   {RootFields, Fields} = prepare_paths(Fields0),
   {RootTags, Tags} = prepare_paths(Tags0),
   {ok, all,
      #state{field_kvs = lists:zip(Fields, FieldV), tag_kvs = lists:zip(Tags, TagV),
         fields = Fields, node_id = NodeId, tags = Tags,
         tag_values = TagV, field_values = FieldV, fields_root = RootFields, tags_root = RootTags}}.

process(_In, Item, State = #state{tag_kvs = []}) ->
   {emit, set_fields(Item, State), State};
process(_In, Item, State = #state{field_kvs = []}) ->
   {emit, set_tags(Item, State), State};
process(_In, Item, State = #state{}) ->
   NewItem0 = set_fields(Item, State),
   NewItem = set_tags(NewItem0, State),
   {emit, NewItem, State}.

set_fields(Item = #data_batch{points = Points}, S = #state{fields_root = true}) ->
   NewPoints = [set_fields(P, S) || P <- Points],
   Item#data_batch{points = NewPoints};
set_fields(Item = #data_point{fields = Fields}, #state{fields_root = true, field_kvs = FieldList}) ->
   NewFields = maps:merge(Fields, maps:from_list(FieldList)),
   Item#data_point{fields = NewFields};
set_fields(Item, #state{field_kvs = FieldList}) ->
   flowdata:set_fields(Item, FieldList).


set_tags(Item = #data_batch{points = Points}, S = #state{tags_root = true}) ->
   NewPoints = [set_tags(P, S) || P <- Points],
   Item#data_batch{points = NewPoints};
set_tags(Item = #data_point{tags = Tags}, #state{tags_root = true, tag_kvs = TagList}) ->
   lager:notice("set tags: ~p",[]),
   NewTags = maps:merge(Tags, maps:from_list(TagList)),
   Item#data_point{tags = NewTags};
set_tags(Item, #state{tag_kvs = TagList}) ->
   flowdata:set_tags(Item, TagList).


-spec prepare_paths(list(binary())) -> {list(), list()}.
prepare_paths(Paths) ->
   AllPaths = [flowdata:path(F) || F <- Paths],
   IsRootAll = lists:all(fun(E) -> flowdata:is_root_path(E) end, AllPaths),
   {IsRootAll, AllPaths}.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
set_roots_test() ->
   S = #state{fields_root = true,
         field_kvs = [{<<"field1">>, <<"value1">>}, {<<"field2">>, <<"value2">>}]},
   Point = #data_point{ts = 123456, fields = #{<<"field">> => <<"value">>}},
   PNew = #data_point{ts = 123456, fields = #{<<"field">> => <<"value">>,
      <<"field1">> => <<"value1">>, <<"field2">> => <<"value2">>}},
   ?assertEqual(PNew, set_fields(Point, S)).
set_roots_overwrite_test() ->
   S = #state{fields_root = true,
      field_kvs = [{<<"field">>, <<"value00000">>},{<<"field1">>, <<"value1">>}, {<<"field2">>, <<"value2">>}]},
   Point = #data_point{ts = 123456, fields = #{<<"field">> => <<"value">>}},
   PNew = #data_point{ts = 123456, fields = #{<<"field">> => <<"value00000">>,
      <<"field1">> => <<"value1">>, <<"field2">> => <<"value2">>}},
   ?assertEqual(PNew, set_fields(Point, S)).
set_misc_test() ->
   S = #state{fields_root = false,
      field_kvs = [{<<"a.field1">>, <<"value1">>}, {<<"field2">>, <<"value2">>}]},
   Point = #data_point{ts = 123456, fields = #{<<"field">> => <<"value">>}},
   PNew = #data_point{ts = 123456, fields = #{<<"field">> => <<"value">>, <<"a">> => #{
      <<"field1">> => <<"value1">>}, <<"field2">> => <<"value2">>}},
   ?assertEqual(PNew, set_fields(Point, S)).


-endif.