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
   fields_names,
   fields_funs,
   fields_as_names,
   fields_as_funs
}).

options() -> [
   {fields, binary_list, []},
   {as_fields, list, []},
   {tags, binary_list, []},
   {as_tags, binary_list, []}].

check_options() ->
   [{same_length, [fields, as_fields]},
      {same_length, [tags, as_tags]}].

init(NodeId, _Ins, #{fields := Fields, tags := _Tags, as_fields := AsFields, as_tags := _AsTags}) ->
   {FieldsAsFuns, FieldsAsNames} = lists:partition(fun({_FName, E}) -> is_function(E) end, lists:zip(Fields, AsFields)),
   {AsNameFields, AsNameFieldNames} = lists:unzip(FieldsAsNames),
   {AsFunFields, AsFunFieldNames} = lists:unzip(FieldsAsFuns),
   {ok, all, #state{
      node_id = NodeId,
      fields_names = AsNameFields,
      fields_funs = AsFunFieldNames,
      fields_as_names = AsNameFieldNames,
      fields_as_funs = AsFunFields}}.

process(_In, #data_batch{points = Points} = Batch,
%%    State = #state{fields = FNames, as_fields = AsFields, tags = Tags, as_tags = AsTags}) ->
    State = #state{fields_names = FNames, fields_as_names = AsFields}) ->
   NewPoints = lists:map(
      fun(P) ->
         NewPoint = flowdata:rename_fields(P, FNames, AsFields),
%%         NewPoint1 = flowdata:rename_tags(NewPoint, Tags, AsTags),
         eval_funs(NewPoint, State)
      end,
      Points
   ),
   {emit, Batch#data_batch{points = NewPoints}, State};

process(_Inport, #data_point{} = Point,
    State = #state{fields_names = FNames, fields_as_names = AsFields}) ->

   NewPoint0 = flowdata:rename_fields(Point, FNames, AsFields),
%%   NewPoint1 = flowdata:rename_tags(NewPoint0, Tags, AsTags),
   NewPoint = eval_funs(NewPoint0, State),
   {emit, NewPoint, State}.

eval_funs(Point = #data_point{}, State = #state{fields_as_funs = []}) ->
   Point;
eval_funs(Point = #data_point{}, #state{fields_as_funs = Funs, fields_funs = Fields}) ->
   do_eval_funs(Funs, Fields, Point, [], []).

do_eval_funs([], [], Point, RenameFields, NewNames) ->
   flowdata:rename_fields(Point, RenameFields, NewNames);
do_eval_funs([FieldName|Names], [Fun|Funs], Point = #data_point{fields = PFields}, RenameFields, NewNames) ->
   NPoint = Point#data_point{fields = PFields#{<<"__fieldname">> => FieldName}},
   NewName = faxe_lambda:execute(NPoint, Fun),
   do_eval_funs(Funs, Names, Point, [FieldName|RenameFields], [NewName|NewNames]).


