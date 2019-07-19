%% Date: 28.01.17 - 14:11
%% Ⓒ 2017 heyoka
%% @doc
%% Add a field and/or a tag if it does not already exist for a datapoint
%% @end
-module(esp_default).
-author("Alexander Minichmair").

%% API
-behavior(df_component).

-include("faxe.hrl").
%% API
-export([init/3, process/3, options/0]).

-record(state, {
   node_id,
   field,
   tag,
   field_value,
   tag_value
}).

options() -> [{field, binary, undefined}, {field_value, number, undefined}, {tag, binary, undefined},
   {tag_value, binary, undefined}].

init(NodeId, _Ins, #{field := Fields, tag := Tags, tag_value := TagV, field_value := FieldV}) ->
   {ok, all, #state{field = Fields, node_id = NodeId, tag = Tags, tag_value = TagV, field_value = FieldV}}.

process(_In, #data_batch{points = Points} = Batch, State = #state{field = FName, field_value = FValue,
   tag = TName, tag_value = TValue}) ->
   NewPoints = lists:map(
      fun(P) ->
         NewPoint = set_field(P, FValue, FName),
         set_tag(NewPoint, TName, TValue)
      end,
      Points
   ),
   {emit, Batch#data_batch{points = NewPoints}, State};
process(_Inport, #data_point{} = Point, State = #state{field = FName, field_value = FValue,
      tag = TName, tag_value = TValue}) ->

   NewPoint0 = set_field(Point, FName, FValue),
   NewPoint = set_tag(NewPoint0, TName, TValue),
%%   lager:info("~p emits : ~p",[?MODULE, NewPoint]),
   {emit, NewPoint, State}.


%%%
%%% @doc
%%% sets a field with a value, if not already defined
%%% @end
-spec set_field(#data_point{}, binary()|undefined,  term()) -> #data_point{}.
set_field(P=#data_point{}, undefined, _) ->
   P;
set_field(P=#data_point{}, FieldName, FieldValue) ->
   case flowdata:field(P, FieldName) of
      undefined -> flowdata:set_field(P, FieldName, FieldValue);
      _ -> P
   end.


%%%
%%% @doc
%%% sets a tag with a value, if not already defined
%%% @end
-spec set_tag(#data_point{}, binary()|undefined,  term()) -> #data_point{}.
set_tag(P=#data_point{}, undefined, _) ->
   P;
set_tag(P=#data_point{}, TagName, TagValue) ->
   case flowdata:tag(P, TagName) of
      undefined -> flowdata:set_tag(P, TagName, TagValue);
      _ -> P
   end.

