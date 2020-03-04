%% Date: 28.01.17 - 14:11
%% Ⓒ 2017 heyoka
%% @doc
%% Add a field and/or a tag if it does not already exist
%% @end
-module(esp_default).
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
   root_fields,
   root_tags
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
   %% optimize field_lookup by translating the possibly deep path to its tuple form and if all paths are
   %% just root paths, we work directly on the underlying map structure
   {RootFields, Fields} = prepare_paths(Fields0),
   {RootTags, Tags} = prepare_paths(Tags0),

   {ok, all,
      #state{fields = Fields, node_id = NodeId, tags = Tags,
         tag_values = TagV, field_values = FieldV, root_fields = RootFields, root_tags = RootTags}}.

process(_In, #data_batch{points = Points} = Batch,
    State = #state{fields = FName, field_values = FValue, root_fields = FieldsRoot,
                  tags = TName, tag_values = TValue, root_tags = TagsRoot}) ->
%%   lager:notice("Points: ~p",[Points]),
   NewPoints = lists:map(
      fun(P) ->
         NewPoint = set_fields(P, FName, FValue, FieldsRoot),
         set_tags(NewPoint, TName, TValue, TagsRoot)
      end,
      Points
   ),
   {emit, Batch#data_batch{points = NewPoints}, State};
process(_Inport, #data_point{} = Point, State = #state{fields = FName, field_values = FValue,
      tags = TName, tag_values = TValue, root_fields = FieldsRoot, root_tags = TagsRoot}) ->

   NewPoint0 = set_fields(Point, FName, FValue, FieldsRoot),
   NewPoint = set_tags(NewPoint0, TName, TValue, TagsRoot),
   {emit, NewPoint, State}.


%%%
%%% @doc
%%% sets a field with a value, if not already defined
%%% @end
-spec set_fields(#data_point{}, list(), list(), true|false) -> #data_point{}.
set_fields(P=#data_point{}, [], _, _) ->
   P;
set_fields(P=#data_point{}, [FieldName|Fn], [FieldValue|Fv], false) ->
   NewPoint =
   case flowdata:field(P, FieldName) of
      undefined -> flowdata:set_field(P, FieldName, FieldValue);
      _ -> P
   end,
   set_fields(NewPoint, Fn, Fv, false);
set_fields(P=#data_point{fields = PFields}, [FieldName|Fn], [FieldValue|Fv], true) ->
   NewPoint =
      case maps:is_key(FieldName, PFields) of
         false -> P#data_point{fields = maps:put(FieldName, FieldValue, PFields)};
         true -> P
      end,
   set_fields(NewPoint, Fn, Fv, true).

%%%
%%% @doc
%%% sets a tag with a value, if not already defined
%%% @end
-spec set_tags(#data_point{}, list(), list(), true|false) -> #data_point{}.
set_tags(P=#data_point{}, [], [], _) ->
   P;
set_tags(P=#data_point{}, [TagName|Tn], [TagValue|Tv], false) ->
   NewP =
   case flowdata:tag(P, TagName) of
      undefined -> flowdata:set_tag(P, TagName, TagValue);
      _ -> P
   end,
   set_tags(NewP, Tn, Tv, false);
set_tags(P=#data_point{tags = PTags}, [TagName|Tn], [TagValue|Tv], true) ->
   NewP =
      case maps:is_key(TagName, PTags) of
         false -> P#data_point{tags = maps:put(TagName, TagValue, PTags)};
         true -> P
      end,
   set_tags(NewP, Tn, Tv, true).


-spec prepare_paths(list(binary())) -> {list(), list()}.
prepare_paths(Paths) ->
   AllPaths = [flowdata:path(F) || F <- Paths],
   IsRootAll = lists:all(fun(E) -> flowdata:is_root_path(E) end, AllPaths),
   {IsRootAll, AllPaths}.


-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
set_roots_test() ->
   Fields = [<<"field1">>, <<"field2">>],
   Values = [<<"value1">>, <<"value2">>],
   Point = #data_point{ts = 123456, fields = #{<<"field">> => <<"value">>, <<"field1">> => <<"value001">>}},
   PNew = #data_point{ts = 123456, fields = #{<<"field">> => <<"value">>,
      <<"field1">> => <<"value001">>, <<"field2">> => <<"value2">>}},
   ?assertEqual(PNew, set_fields(Point, Fields, Values, true)).

set_misc_test() ->
   Fields = [<<"field1">>, <<"a.field2">>],
   Values = [<<"value1">>, <<"value2">>],
   Point = #data_point{ts = 123456, fields = #{<<"field">> => <<"value">>, <<"field1">> => <<"value001">>}},
   PNew = #data_point{ts = 123456, fields = #{<<"field">> => <<"value">>,
      <<"field1">> => <<"value001">>, <<"a">> => #{<<"field2">> => <<"value2">>}}},
   ?assertEqual(PNew, set_fields(Point, Fields, Values, false)).


-endif.