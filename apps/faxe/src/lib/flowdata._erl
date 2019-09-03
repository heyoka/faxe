%% Date: 07.01.17 - 22:41
%% Ⓒ 2017 heyoka
%%
%% @doc
%% This module provides functions for all needs in context with #data_point and #data_batch records.
%%
%% Field and Tag names in a data_point are always binary strings.
%% Field values are usually int or float values.
%% Tag values a always binary strings.
%% Every data_point record has a ts-field it's value is always a unix-timestamp in
%% millisecond precision.
%%
%% A data_batch record consists of an ordered list of data_point records (field 'points').
%% The 'points' list is ordered by timestamp such that the oldest point is the last entry in the list.
%% @end

-module(flowdata).
-author("Alexander Minichmair").

-include("faxe.hrl").
%% API
-export([
   %% batch and point
   ts/1, to_map/1, to_msgpack/1, to_json/1,
   field/2,
   tag/2,
   id/1,
   set_field/3,
   set_tag/3,
   value/2, values/2, delete_field/2, delete_tag/2,
   %% batch only
   first_ts/1, first/1, take/1, take2/1, set_bounds/1,
   %% point only
   set_ts/2,
   field_names/1, tag_names/1,
   rename_fields/3, rename_tags/3,
   expand_json_field/2, extract_map/2]).


-spec to_json(P :: #data_batch{}|#data_point{}) -> jsx:json_text().
to_json(P) when is_record(P, data_point) orelse is_record(P, data_batch) ->
   jsx:encode(to_map(P)).

-spec to_msgpack(P :: #data_point{}|#data_batch{}) -> binary() | {error, {badarg, term()}}.
to_msgpack(P) when is_record(P, data_point) orelse is_record(P, data_batch) ->
   msgpack:pack(to_map(P)).

-spec to_map(#data_point{}|#data_batch{}) -> map()|list(map()).
to_map(#data_point{ts = Ts, fields = Fields, tags = Tags}) ->
   M = maps:merge(maps:from_list(Fields), maps:from_list(Tags)),
   M#{<<"ts">> => Ts};
to_map(#data_batch{points = Points}) ->
   [to_map(P) || P <- Points].

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
   Map = jsx:decode(JSONVal),
   P0#data_point{fields = P0#data_point.fields ++ Map}.

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
-spec value(#data_point{}, binary()) -> term()|undefined.
value(#data_point{ts = Ts}, <<"ts">>) ->
   Ts;
value(#data_point{fields = Fields, tags = Tags}, F) ->
   case proplists:get_value(F,Fields) of
      undefined -> proplists:get_value(F,Tags);
      Value -> Value
   end.
%%
%% @doc get the values with a key from data_batch fields or tags @end
%%
-spec values(#data_batch{}, binary()) -> list(term()|undefined).
values(#data_batch{points = Points}, F) ->
   [value(Point, F) || Point <- Points].

%%
%% @doc get the values with a key from field(s) @end
%%
-spec field(#data_point{}|#data_batch{}, binary()) -> undefined | term() | list(term()|undefined).
field(#data_point{fields = Fields}, F) ->
   proplists:get_value(F,Fields)
;
field(#data_batch{points = Points}, F) ->
   [field(Point, F) || Point <- Points].

%% @doc
%% get an unordered list of all fieldnames from the given data_point
%% @end
-spec field_names(#data_point{}) -> list().
field_names(#data_point{fields = Fields}) ->
   proplists:get_keys(Fields).

%% @doc
%% get an unordered list of all tag-names from the given data_point
%% @end
-spec tag_names(#data_point{}) -> list().
tag_names(#data_point{tags = Tags}) ->
   proplists:get_keys(Tags).

%%%% setter

%% @doc
%% set the timestamp for the given data_point
%% @end
-spec set_ts(#data_point{}, non_neg_integer()) -> #data_point{}.
set_ts(P=#data_point{}, NewTs) ->
   P#data_point{ts = NewTs}.

%% @doc
%% Set the field Key to Value.
%% If a field named Key does exist, it will be overwritten.
%% @end
-spec set_field(#data_point{}, binary(), any()) -> #data_point{}.
set_field(P = #data_point{fields = Fields}, Key, Value) ->
   NewFields = set_field(Key, Value, Fields),
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
   P#data_batch{points = Ps};
%% @doc 
%% set a key value pair into a fieldlist (which is a proplist)
%% if an entry with Key exists already, then the entry will be updated,
%% otherwise the entry will be appended to the fieldlist
%% @end
set_field(Key, Value, FieldList) when is_binary(Key), is_list(FieldList) ->
   FList =
      case proplists:get_value(Key, FieldList, nil) of
         nil   -> FieldList;
         _     -> proplists:delete(Key, FieldList)
      end,
   [{Key, Value} | FList].


-spec tag(#data_point{}|#data_batch{}, binary()) -> undefined | term() | list(term()|undefined).
%%% @doc
%%% get a tags' value
%%% @end
tag(#data_point{tags = Fields}, F) ->
   proplists:get_value(F,Fields)
;
tag(#data_batch{points = Points}, F) ->
   [tag(Point, F) || Point <- Points].

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
   NewFields = set_field(Key, Value, Fields),
   P#data_point{tags = NewFields}
;
set_tag(#data_batch{points = Points}, Key, Value) ->
   lists:map(
      fun(#data_point{} = D) ->
         set_tag(D, Key, Value)
      end,
      Points
   ).

%% @doc
%% delete a field with the given name, if a data_batch record is provided, the field gets deleted from all
%% data_points in the data_batch
%% @end
-spec delete_field(#data_point{}|#data_batch{}, binary()) -> #data_point{} | #data_batch{}.
delete_field(#data_point{fields = Fields}=P, FieldName) ->
   P#data_point{fields = proplists:delete(FieldName, Fields)}
   ;
delete_field(#data_batch{points = Points}=B, FieldName) ->
   NewPoints = [delete_field(Point, FieldName) || Point <- Points],
   B#data_batch{points = NewPoints}.

%% @doc
%% delete a tag with the given name, if a data_batch record is provided, the tag gets deleted from all
%% data_points in the data_batch
%% @end
-spec delete_tag(#data_point{}|#data_batch{}, binary()) -> #data_point{} | #data_batch{}.
delete_tag(#data_point{tags = Tags}=P, TagName) ->
   P#data_point{tags = proplists:delete(TagName, Tags)}
;
delete_tag(#data_batch{points = Points}=B, TagName) ->
   NewPoints = [delete_field(Point, TagName) || Point <- Points],
   B#data_batch{points = NewPoints}.

%% @doc
%% rename fields and tags
%% @end
-spec rename_fields(#data_point{}, list(binary()), list(binary())) -> #data_point{}.
rename_fields(#data_point{fields = Fields} = P, FieldNames, Aliases) ->
   P#data_point{fields = rename(Fields, FieldNames, Aliases)}.

rename_tags(#data_point{tags = Tags} = P, TagNames, Aliases) ->
   P#data_point{tags = rename(Tags, TagNames, Aliases)}.

rename(List, From, To) when is_list(List), is_list(From), is_list(To)->
   proplists:substitute_aliases(lists:zip(From, To), List).
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


%%%%%%%%%%%%%%%%%%%%%% destructive operations !!!
-spec first(#data_batch{}) -> #data_point{} | eol.
first(#data_batch{points = []}) ->
   eol;
first(#data_batch{points = [First | _R]}) ->
   First.

-spec take(#data_batch{}) -> {#data_point{}, #data_batch{}} | {eol, #data_batch{}}.
take(#data_batch{points = []} = Batch) ->
   {eol, Batch};
take(#data_batch{points = [First | R]} = Batch) ->
   {First, Batch#data_batch{points = R}}.

-spec take2(#data_batch{}) -> {{#data_point{},#data_point{}}, #data_batch{}} | {eol, #data_batch{}}.
take2(#data_batch{points = []} = Batch) ->
   {eol, Batch};
take2(#data_batch{points = [_V]} = Batch) ->
   {eol, Batch};
take2(#data_batch{points = [First | [Sec|R]]} = Batch) ->
   {{First, Sec}, Batch#data_batch{points = R}}.




%%%%%%%%%%%%%%%%%%% INTERNAL %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

sample_point() ->
   #data_point{ts = 1231154646, fields=[{f1, 223.3},{f2, 44},{f3, 2}],
      tags=[{t1, <<"hello">>},{t2, <<"JU323z6">>}]}.
-ifdef(TEST).


datapoint_to_map_test() ->
   ?assertEqual(flowdata:to_map(sample_point()), #{f1 => 223.3,f2 => 44,f3 => 2,t1 => <<"hello">>,
      t2 => <<"JU323z6">>,<<"ts">> => 1231154646}).
-endif.



