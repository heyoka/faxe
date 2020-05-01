%% Date: 07.01.17 - 22:41
%% Ⓒ 2017 heyoka
%%
%% @doc
%% This module provides functions for all needs in context with #data_point and #data_batch records.
%%
%% -record(data_point, {
%%    ts                :: non_neg_integer(), %% timestamp in ms
%%    fields   = #{}    :: map(),
%%    tags     = #{}    :: map(),
%%    id       = <<>>   :: binary()
%% }).
%%
%% -record(data_batch, {
%%    id                :: binary(),
%%    points            :: list(#data_point{}),
%%    start             :: non_neg_integer(),
%%    ed                :: non_neg_integer()
%% }).
%%
%% The basic data_type for the record-fields 'fields' and 'tags' is a map.
%% Note: for 'tags' it is assumed, that the map is 1-dimensional
%%
%% Field and Tag names in a data_point are always binary strings.
%% Field values are simple int, string or float values, but can also be deep nested maps and lists
%%
%% Tag values a always binary strings.
%%
%% Every data_point record has a ts-field it's value is always a unix-timestamp in
%% millisecond precision.
%%
%% For every function that expects a name/path (binary) a jsonpath query can be provided, ie:
%%
%% * flowdata:field(#data_point{}, <<"value.sub[2].data">>). will return the value at value.sub[2].data
%% * flowdata:field(#data_point{}, <<"averages">>).
%% * flowdata:field(#data_point{}, <<"averages.emitted[5]">>).
%%
%% A data_batch record consists of an ordered (by timestamp) list of data_point records (field 'points').
%% The 'points' list is ordered by timestamp such that the oldest point is the last entry in the list.
%% @end

-module(flowdata).
-author("Alexander Minichmair").

-include("faxe.hrl").

%% API
-export([
   %% batch and point
   ts/1, to_json/1,
   field/2,
   tag/2,
   id/1,
   set_field/3,
   set_tag/3,
   value/2, values/2, delete_field/2, delete_tag/2,
   %% batch only
   first_ts/1, set_bounds/1,
   %% point only
   set_ts/2,
   field_names/1, tag_names/1,
   rename_fields/3, rename_tags/3,
   expand_json_field/2, extract_map/2, extract_field/3,
   field/3, to_s_msgpack/1, from_json/1,
   to_map/1, set_fields/3, set_tags/3, fields/2, tags/2,
   delete_fields/2, delete_tags/2, path/1, paths/1, set_fields/2,
   to_mapstruct/1, from_json/2, from_json_struct/1, from_json_struct/3,
   to_map_except/2, point_from_json_map/1, point_from_json_map/3,
   merge_points/1, set_tags/2, is_root_path/1, merge/2]).


-define(DEFAULT_ID, <<"00000">>).
-define(DEFAULT_VS, 1).
-define(DEFAULT_DF, <<"00.000">>).

-define(DEFAULT_FIELDS, [<<"id">>, <<"vs">>, <<"df">>, <<"ts">>]).
-define(DEFAULT_TS_FIELD, <<"ts">>).


to_json(P) when is_record(P, data_point) orelse is_record(P, data_batch) ->
%%   lager:notice("MapStruct: ~p", [to_mapstruct(P)]),
   case jiffy:encode(to_mapstruct(P), []) of
      Bin when is_binary(Bin) -> Bin;
      IoList when is_list(IoList) -> iolist_to_binary(IoList)
   end.
%%
%% empty message, when there is no field set (empty map)
to_mapstruct(#data_point{ts = _Ts, fields = Fields, tags = _Tags}) when map_size(Fields) == 0 ->
   #{};
to_mapstruct(P=#data_point{ts = Ts, fields = Fields, tags = _Tags}) ->

   AllDataFields = maps:without([<<"id">>,<<"vs">>,<<"df">>], Fields),
   DataFields =
      case maps:get(<<"data">>, AllDataFields, nil) of
         nil -> AllDataFields;
         Data -> maps:merge(maps:without([<<"data">>], AllDataFields), Data)
      end,
   #{?DEFAULT_TS_FIELD => Ts,
      <<"id">> => field(P ,<<"id">>, ?DEFAULT_ID),
      <<"vs">> => field(P, <<"vs">>, ?DEFAULT_VS),
      <<"df">> => field(P, <<"df">>, ?DEFAULT_DF),
      <<"data">> => DataFields};

to_mapstruct(_B=#data_batch{points = Points}) ->
   [to_mapstruct(P) || P <- Points].

-spec to_s_msgpack(P :: #data_point{}|#data_batch{}) -> binary() | {error, {badarg, term()}}.
to_s_msgpack(P) when is_record(P, data_point) orelse is_record(P, data_batch) ->
   msgpack:pack(to_mapstruct(P), [{map_format, jiffy}]).

from_json(JSONMessage, _FieldMapping) ->
   Map = from_json(JSONMessage),
   Data = maps:without(?DEFAULT_FIELDS, Map),
   set_field(#data_point{}, <<"data">>, Data).

-spec from_json_struct(binary()) -> #data_point{}|#data_batch{}.
from_json_struct(JSON) ->
   from_json_struct(JSON, ?DEFAULT_TS_FIELD, ?TF_TS_MILLI).

-spec from_json_struct(binary(), binary(), binary()) -> #data_point{}|#data_batch{}.
from_json_struct(JSON, TimeField, TimeFormat) ->
   Struct = from_json(JSON),
   case Struct of
      Map when is_map(Map) ->
         point_from_json_map(Map, TimeField, TimeFormat);
      List when is_list(List) ->
         Points = [point_from_json_map(PMap, TimeField, TimeFormat) || PMap <- List],
         #data_batch{points = Points}
   end.

%% from a map get a data_point record
-spec point_from_json_map(map()) -> #data_point{}.
point_from_json_map(Map) ->
   point_from_json_map(Map, ?DEFAULT_TS_FIELD, ?TF_TS_MILLI).

-spec point_from_json_map(map(), binary(), binary()) -> #data_point{}.
point_from_json_map(Map, TimeField, TimeFormat) ->
   Ts0 = maps:get(TimeField, Map, undefined),
   Ts =
      case Ts0 of
         undefined -> faxe_time:now();
         Timestamp -> time_format:convert(Timestamp, TimeFormat)
      end,
   Data = maps:without(?DEFAULT_FIELDS, Map),
   Fields = maps:remove(?DEFAULT_TS_FIELD, maps:with(?DEFAULT_FIELDS, Map)),
   Point = #data_point{ts = Ts, fields = Data},
   set_fields(Point, maps:to_list(Fields)).

from_json(Message) ->
   try jiffy:decode(Message, [return_maps, dedupe_keys]) of
      Json when is_map(Json) orelse is_list(Json) -> Json
   catch
      _:_ -> #{}
   end.

%% return a pure map representation from a data_point/data_batch, adds a timestamp as <<"ts">>
-spec to_map(#data_point{}|#data_batch{}) -> map()|list(map()).
to_map(#data_point{ts = Ts, fields = Fields, tags = Tags}) ->
   M = maps:merge(Fields, Tags),
   M#{<<"ts">> => Ts};
to_map(#data_batch{points = Points}) ->
   [to_map(P) || P <- Points].

%% return a map representation of #data_point without the given keys
to_map_except(P=#data_point{}, Without) when is_list(Without) ->
   maps:without([[<<"ts">>]|Without], to_map(P)).

%% extract a given map into the fields-list in data_point P
%% return the updated data_point
-spec extract_map(#data_point{}, map()) -> #data_point{}.
extract_map(P = #data_point{fields = Fields}, Map) when is_map(Map) ->
   List = maps:to_list(Map),
   lager:notice("maps:to_list: ~p",[List]),
   P#data_point{fields = Fields ++ List}.

expand_json_field(P = #data_point{}, FieldName) ->
   JSONVal = field(P, FieldName),
   P0 = delete_field(P, FieldName),
   Map = jiffy:decode(JSONVal, [return_maps]),
   P0#data_point{fields = P0#data_point.fields ++ Map}.

%% @doc merge a list of data_points into one
merge_points([#data_point{ts=Ts} |_Ps] = Points) ->
   FMaps = [P#data_point.fields || P <- Points],
   Fields = merge_fields(FMaps, #{}),
   #data_point{ts = Ts, fields = Fields}.
merge_fields([], Acc) ->
   Acc;
merge_fields([F1|Fields], Acc) ->
   merge_fields(Fields, maps:merge(F1, Acc)).

%% @doc
%% get the timestamp from the given field
%% @end
-spec ts(#data_point{}|#data_batch{}) -> non_neg_integer() | list(non_neg_integer()).
ts(#data_point{ts = Ts}) ->
   Ts
;
%% @doc
%% get a list of all timestamps from all fields in the data_batch
%% @end
ts(#data_batch{points = Points}) ->
   [ts(Point) || Point <- Points].

%%
%% @doc
%% get the value with a key from a #data_point field or tag,
%%
%% or get the points timestamp (this is used when no context for processing is given,
%% ie.: in a lambda fun)
%% @end
%%
-spec value(#data_point{}, jsonpath:path()) -> term()|undefined.
value(#data_point{ts = Ts}, <<"ts">>) ->
   Ts;
value(#data_point{fields = Fields, tags = Tags}, F) ->
   Path = path(F),
   case jsn_get(Path, Fields) of
      undefined -> jsn_get(Path, Tags);
      Value -> Value
   end.
%%
%% @doc get the values with a key from data_batch fields or tags @end
%%
-spec values(#data_batch{}, jsonpath:path()) -> list(term()|undefined).
values(#data_batch{points = Points}, F) ->
   [value(Point, F) || Point <- Points].

%%
%% @doc get the values with a key from field(s) @end
%%
-spec field(#data_point{}|#data_batch{}, binary(), term()) -> term().
field(#data_point{fields = Fields}, F, Default) ->
   jsn_get(F, Fields, Default)
;
field(#data_batch{points = Points}, F, Default) ->
   [field(Point, F, Default) || Point <- Points].

-spec field(#data_point{}|#data_batch{}, jsonpath:path()) -> undefined | term() | list(term()|undefined).
field(#data_point{fields = Fields}, F) ->
   jsn_get(F, Fields)
;
field(#data_batch{points = Points}, F) ->
   [field(Point, F) || Point <- Points].

%% @doc get a list of field-values with a list of keys/paths
fields(#data_point{fields = Fields}, PathList) when is_list(PathList) ->
   jsn_getlist(PathList, Fields).

%% @doc
%% get an unordered list of all fieldnames from the given data_point
%% @end
-spec field_names(#data_point{}) -> list().
field_names(#data_point{fields = Fields}) ->
   maps:keys(Fields).

%% @doc
%% get an unordered list of all tag-names from the given data_point
%% @end
-spec tag_names(#data_point{}) -> list().
tag_names(#data_point{tags = Tags}) ->
   maps:keys(Tags).

%%%% setter

%% @doc
%% set the timestamp for the given data_point
%% @end
-spec set_ts(#data_point{}, non_neg_integer()) -> #data_point{}.
set_ts(P=#data_point{}, NewTs) ->
   P#data_point{ts = NewTs}.

%% @doc
%% Set the field Key to Value with path.
%% If a field named Key does exist, it will be overwritten.
%% @end
-spec set_field(#data_point{}, jsonpath:path(), any()) -> #data_point{}.
%% special version timestamp field
set_field(P = #data_point{}, <<"ts">>, Value) ->
   P#data_point{ts = Value}
;
set_field(P = #data_point{fields = Fields}, Key, Value) ->
%%   lager:notice("set_field(~p, ~p, ~p)", [Fields, Key, Value]),
   NewFields = set(Key, Value, Fields),
   P#data_point{fields = NewFields}
;
%%%
%%% @doc
%%% set field with name Key to every Value in the databatch
%%% @end
%%
set_field(P = #data_batch{points = Points}, Key, Value) ->
   Ps = lists:map(
     fun(#data_point{} = D) ->
        set_field(D, Key, Value)
     end,
      Points
   ),
   P#data_batch{points = Ps}.

%% set multiple fields at once
-spec set_fields(#data_point{}|#data_batch{}, list(), list()) -> #data_point{}|#data_batch{}.
set_fields(P = #data_point{fields = Fields}, Keys, Values) when is_list(Keys), is_list(Values) ->
   NewFields = jsn_setlist(Keys, Values, Fields),
   P#data_point{fields = NewFields};
set_fields(B = #data_batch{points = Points}, Keys, Values) when is_list(Keys), is_list(Values) ->
   Ps = lists:map(
      fun(#data_point{} = D) ->
         set_fields(D, Keys, Values)
      end,
      Points
   ),
   B#data_batch{points = Ps}.

set_fields(P = #data_point{fields = Fields}, KeysValues) when is_list(KeysValues) ->
   NewFields = jsn_setlist(KeysValues, Fields),
   P#data_point{fields = NewFields};
set_fields(B = #data_batch{points = Points}, KeysValues) when is_list(KeysValues) ->
   Ps = lists:map(
      fun(#data_point{} = D) ->
         set_fields(D, KeysValues)
      end,
      Points
   ),
   B#data_batch{points = Ps}.

%% @doc 
%% set a key value pair into a fieldlist (which is a map)
%% if an entry with Key exists already, then the entry will be updated,
%% otherwise the entry will be appended to the fieldlist
%% @end
-spec set(jsonpath:path(), term(), list()) -> list().
set(Key, Value, FieldList) ->
   jsn_set(Key, Value, FieldList).

-spec tag(#data_point{}|#data_batch{}, jsonpath:path()) -> undefined | term() | list(term()|undefined).
%%% @doc
%%% get a tags' value
%%% @end
tag(#data_point{tags = Fields}, F) ->
   jsn_get(F, Fields)
;
tag(#data_batch{points = Points}, F) ->
   [tag(Point, F) || Point <- Points].

%% @doc get a list of field-values with a list of keys/paths
tags(#data_point{tags = Tags}, PathList) when is_list(PathList) ->
   jsn_getlist(PathList, Tags).

%% @doc
%% get the id from the given data_point or data_batch
%% @end
-spec id(#data_batch{}) -> any().
id(#data_batch{id = Id}) ->
   Id;
id(#data_point{id = Id}) ->
   Id.

%% @doc
%% set a tag to a given value, or set all tags in all points to a given value
%% @end
-spec set_tag(#data_batch{}|#data_point{}, binary(), binary()) -> #data_batch{}|#data_point{}.
set_tag(P = #data_point{tags = Fields}, Key, Value) ->
   NewFields = set(Key, Value, Fields),
   P#data_point{tags = NewFields}
;
set_tag(B = #data_batch{points = Points}, Key, Value) ->
   Ps = lists:map(
      fun(#data_point{} = D) ->
         set_tag(D, Key, Value)
      end,
      Points
   ),
   B#data_batch{points = Ps}.

%% set multiple tags at once
-spec set_tags(#data_point{}|#data_batch{}, list(), list()) -> #data_point{}|#data_batch{}.
set_tags(P = #data_point{tags = Tags}, Keys, Values) when is_list(Keys), is_list(Values) ->
   NewTags = jsn_setlist(Keys, Values, Tags),
   P#data_point{tags = NewTags};
set_tags(B = #data_batch{points = Points}, Keys, Values) when is_list(Keys), is_list(Values) ->
   Ps = lists:map(
      fun(#data_point{} = D) ->
         set_tags(D, Keys, Values)
      end,
      Points
   ),
   B#data_batch{points = Ps}.

set_tags(P = #data_point{tags = Tags}, KeysValues) when is_list(KeysValues) ->
   NewTags = jsn_setlist(KeysValues, Tags),
   P#data_point{tags = NewTags};
set_tags(B = #data_batch{points = Points}, KeysValues) when is_list(KeysValues) ->
   Ps = lists:map(
      fun(#data_point{} = D) ->
         set_tags(D, KeysValues)
      end,
      Points
   ),
   B#data_batch{points = Ps}.

%% @doc
%% delete a field with the given name, if a data_batch record is provided, the field gets deleted from all
%% data_points in the data_batch
%% @end
-spec delete_field(#data_point{}|#data_batch{}, binary()) -> #data_point{} | #data_batch{}.
delete_field(#data_point{fields = Fields}=P, FieldName) ->
   case field(P, FieldName) of
      undefined -> P;
      _Else -> JData = jsn_delete(FieldName, Fields),
         P#data_point{fields = JData}
   end
   ;
delete_field(#data_batch{points = Points}=B, FieldName) ->
   NewPoints = [delete_field(Point, FieldName) || Point <- Points],
   B#data_batch{points = NewPoints}.

%% @doc delete a list of keys/paths
-spec delete_fields(#data_point{}|#data_batch{}, list()) -> #data_point{}|#data_batch{}.
delete_fields(P = #data_point{fields = Fields}, KeyList) when is_list(KeyList) ->
   NewFields = jsn_deletelist(KeyList, Fields),
   P#data_point{fields = NewFields};
delete_fields(B = #data_batch{points = Points}, KeyList) when is_list(KeyList) ->
   Ps = lists:map(
      fun(#data_point{} = D) ->
         delete_fields(D, KeyList)
      end,
      Points
   ),
   B#data_batch{points = Ps}.

%% @doc
%% delete a tag with the given name, if a data_batch record is provided, the tag gets deleted from all
%% data_points in the data_batch
%% @end
-spec delete_tag(#data_point{}|#data_batch{}, binary()) -> #data_point{} | #data_batch{}.
delete_tag(#data_point{tags = Tags}=P, TagName) ->
   P#data_point{tags = jsn_delete(TagName, Tags)}
;
delete_tag(#data_batch{points = Points}=B, TagName) ->
   NewPoints = [delete_tag(Point, TagName) || Point <- Points],
   B#data_batch{points = NewPoints}.

%% @doc delete a list of keys/paths from tags
-spec delete_tags(#data_point{}|#data_batch{}, list()) -> #data_point{}|#data_batch{}.
delete_tags(P = #data_point{tags = Tags}, KeyList) when is_list(KeyList) ->
   NewTags = jsn_deletelist(KeyList, Tags),
   P#data_point{tags = NewTags};
delete_tags(B = #data_batch{points = Points}, KeyList) when is_list(KeyList) ->
   Ps = lists:map(
      fun(#data_point{} = D) ->
         delete_tags(D, KeyList)
      end,
      Points
   ),
   B#data_batch{points = Ps}.

%% @doc
%% rename fields and tags, does no inserting value, when field(s) / tag(s) are not found
%% @end
-spec rename_fields(#data_point{}, list(jsonpath:path()), list(jsonpath:path())) -> #data_point{}.
rename_fields(#data_point{fields = Fields} = P, FieldNames, Aliases) ->
   P#data_point{fields = rename(Fields, lists:reverse(FieldNames), lists:reverse(Aliases))}.

rename_tags(#data_point{tags = Tags} = P, TagNames, Aliases) ->
   P#data_point{tags = rename(Tags, lists:reverse(TagNames), lists:reverse(Aliases))}.

rename(Map, [], []) ->
   Map;
rename(Map, [From|RFrom], [To|RTo]) when is_map(Map) ->
   NewData = do_rename(Map, From, To),
   rename(NewData, RFrom, RTo).

do_rename(Map, From, To) ->
   Val = jsn_get(From, Map, undefined),
   case Val of
      undefined -> Map;
      _Val -> NewData = jsn_delete(From, Map),
         set(To, Val, NewData)
   end.

%% @doc searches for a path-value and returns a new data_point with this value
%% if the given path is not found, returns a Default value
extract_field(P = #data_point{}, Path, FieldName) ->
   extract_field(P, Path, FieldName, 0).

-spec extract_field(#data_point{}, binary(), binary()) -> #data_point{}.
extract_field(P = #data_point{}, Path, FieldName, Default) ->
   NewValue =
   case field(P, Path) of
      undefined -> Default;
      Val -> Val
   end,
   set_field(#data_point{}, FieldName, NewValue).
   


%%%%%%%%%%%%%%%%%%%%%%%% batch only  %%%%%%%%%%%%%%%%%%

set_bounds(B=#data_batch{points = [F|_R]}) ->
   set_first(B#data_batch{ed = ts(F)}).
set_first(B=#data_batch{points = Ps}) ->
   P = lists:last(Ps),
   B#data_batch{start = ts(P)}.

%% @doc
%% get the first (ie oldest) timestamp from a data_batch
%% @end
-spec first_ts(#data_batch{}) -> non_neg_integer().
first_ts(#data_batch{points = P}) ->
   ts(lists:last(P)).




%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%%%%%%%%%%% paths
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
jsn_get(_Path, Map) when map_size(Map) == 0 ->
   undefined;
jsn_get(Path, Map) when is_tuple(Path) ->
   jsn:get(Path, Map);
jsn_get(Path, Map) ->
   jsn:get(path(Path), Map).
jsn_get(_Path, Map, Default) when map_size(Map) == 0 ->
   Default;
jsn_get(Path, Map, Default) ->
   jsn:get(path(Path), Map, Default).
jsn_getlist(_PathList, Empty) when map_size(Empty) == 0  ->
   [];
jsn_getlist(PathList, Map) when is_list(PathList) ->
   jsn:get_list(paths(PathList), Map).

jsn_set(Path, Val, Map) ->
   jsn:set(path(Path), Map, Val).
jsn_setlist(Keys, Values, Map) when is_list(Keys), is_list(Values) ->
   jsn:set_list(lists:zip(paths(Keys), Values), Map).
jsn_setlist([], Map) ->
   Map;
%% is the path a tuple already? then do not convert paths
jsn_setlist([{K, _V}|_]=KeysValues, Map) when is_tuple(K) ->
   lager:info("Key is : ~p",[K]),
   jsn:set_list(KeysValues, Map);
jsn_setlist(KeysValues, Map) when is_list(KeysValues) ->
   {Keys, Values} = lists:unzip(KeysValues),
   jsn_setlist(Keys, Values, Map).

jsn_delete(_Path, Map) when map_size(Map) == 0 ->
   Map;
jsn_delete(Path, Map) ->
   jsn:delete(path(Path), Map).

jsn_deletelist([], Map) ->
   Map;
jsn_deletelist(_Paths, Map) when map_size(Map) == 0 ->
   Map;
jsn_deletelist(Paths, Map) ->
   jsn:delete_list(paths(Paths), Map).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Convert a binary path into its tuple-form, only when array-indices are used in the path.
%% This is required, because binary paths do not support array indices.
%% It is recommended to compile the paths used in your flow-components at startup and use the
%% compiled version in consecutive calls to the flowdata functions
-spec paths(list(binary())) -> list(binary()|tuple()).
paths(Paths) when is_list(Paths) ->
   [path(Path) || Path <- Paths].


-spec path(binary()|tuple()) -> binary()|tuple().
path(Path) when is_tuple(Path) -> Path;
path(Path) when is_binary(Path) ->
   case ( catch ets:lookup(field_paths, Path)) of
      [{Path, Cached}] -> Cached;
      [] -> Ret = convert_path(Path), ets:insert(field_paths, {Path, Ret}), Ret;
      _ -> Ret = convert_path(Path),  Ret
end.

convert_path(Path) ->
   case binary:match(Path, <<"[">>) of
      nomatch -> Path;
      _Match -> Split = binary:split(Path, [<<".">>], [global, trim_all]),
         PathList =
            lists:foldl(fun(E, List) -> List ++ extract_array_index(E) end,
               [],
               Split),
         list_to_tuple(PathList)
   end.

extract_array_index(Bin) ->
   case binary:split(Bin, [<<"[">>,<<"]">>], [global, trim_all]) of
      [Bin] = Out -> Out;
      [Part1, BinIndex] -> [Part1, binary_to_integer(BinIndex)]
   end.

%% @doc whether the given path is a root path, ie: can be set with maps:put to fields or tags
-spec is_root_path(binary()) -> true|false.
is_root_path(Path) when is_binary(Path) ->
   case binary:match(Path, [<<".">>,<<"[">>]) of
      nomatch -> true;
      _ -> false
   end;
is_root_path({Path}) when is_binary(Path) ->
   true;
is_root_path(_) ->
   false.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% map merge funcs

merge(M1, M2) when is_map(M1), is_map(M2) -> mapz:deep_merge(merge_fun(), #{}, [M1, M2]);
merge(M1, M2) when is_list(M1), is_list(M2) -> lists:merge(M1, M2);
merge(V1, V2) when is_number(V1), is_number(V2) -> V1 + V2;
merge(S1, S2) when is_binary(S1), is_binary(S2) -> string:concat(S1, S2);
merge(_, _) -> error(cannot_merge_wrong_or_different_datatypes).
merge_fun() ->
   fun
      (Prev, Val) when is_map(Prev), is_map(Val) -> maps:merge(Prev, Val);
      (Prev, Val) when is_list(Prev), is_list(Val) -> lists:merge(Prev, Val);
      (_, Val) -> Val
   end.