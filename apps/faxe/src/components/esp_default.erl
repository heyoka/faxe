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
   fields,
   tags,
   field_values,
   tag_values
}).

options() -> [
   {field, binary_list, []},
   {field_value, list, []},
   {tag, binary_list, []},
   {tag_value, binary_list, []}].

init(NodeId, _Ins, #{field := Fields, tag := Tags, tag_value := TagV, field_value := FieldV} = Opts) ->
   lager:warning("Opts for esp_default: ~p",[Opts]),
   {ok, all, #state{fields = Fields, node_id = NodeId, tags = Tags, tag_values = TagV, field_values = FieldV}}.

process(_In, #data_batch{points = Points} = Batch, State = #state{fields = FName, field_values = FValue,
   tags = TName, tag_values = TValue}) ->
   NewPoints = lists:map(
      fun(P) ->
         NewPoint = set_fields(P, FValue, FName),
         set_tags(NewPoint, TName, TValue)
      end,
      Points
   ),
   {emit, Batch#data_batch{points = NewPoints}, State};
process(_Inport, #data_point{} = Point, State = #state{fields = FName, field_values = FValue,
      tags = TName, tag_values = TValue}) ->

   NewPoint0 = set_fields(Point, FName, FValue),
   NewPoint = set_tags(NewPoint0, TName, TValue),
   {emit, NewPoint, State}.


%%%
%%% @doc
%%% sets a field with a value, if not already defined
%%% @end
-spec set_fields(#data_point{}, list(), list()) -> #data_point{}.
set_fields(P=#data_point{}, [], _) ->
   P;
set_fields(P=#data_point{}, [FieldName|Fn], [FieldValue|Fv]) ->
   NewPoint =
   case flowdata:field(P, FieldName) of
      undefined -> flowdata:set_field(P, FieldName, FieldValue);
      _ -> P
   end,
   set_fields(NewPoint, Fn, Fv).


%%%
%%% @doc
%%% sets a tag with a value, if not already defined
%%% @end
-spec set_tags(#data_point{}, list(), list()) -> #data_point{}.
set_tags(P=#data_point{}, [], []) ->
   P;
set_tags(P=#data_point{}, [TagName|Tn], [TagValue|Tv]) ->
   NewP =
   case flowdata:tag(P, TagName) of
      undefined -> flowdata:set_tag(P, TagName, TagValue);
      _ -> P
   end,
   set_tags(NewP, Tn, Tv).

