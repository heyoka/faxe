%% Date: 2023-04-06 20:10
%% Ⓒ 2023 heyoka
%%
%% @doc
%% for every given field in 'fields', holds the last value seen for this field
%% output a data-point with all the fields currently in the buffer on every incoming data-item
%% it is possible to set a default value for fields that have not be seen so far
%% timestamp, delivery_tag and all tags of the current incoming data-point will be used in the output point
%% @end
-module(esp_collect_fields).
-author("Alexander Minichmair").


%% API
-behavior(df_component).

-include("faxe.hrl").
%% API
-export([init/3, process/3, options/0
   , check_options/0
   , wants/0, emits/0]).

-record(state, {
   node_id,
   current = #data_point{},
   fields,
   default,
   emit_unchanged,
   keep,
   keep_as
}).

options() -> [
   {fields, string_list},
   {default, any, undefined},
   {emit_unchanged, boolean, true},
   {keep, string_list, []},
   {keep_as, string_list, undefined}
].

check_options() ->
   [
      {same_length, [keep, keep_as]}
   ].

wants() -> point.
emits() -> point.

init(NodeId, _Ins,
    #{fields := Fields, default := Default, emit_unchanged := EmitUnchanged, keep := Keep, keep_as := KeepAs}) ->
   {ok, all,
      #state{
         node_id = NodeId, fields = Fields, default = Default,
         emit_unchanged = EmitUnchanged, keep = Keep, keep_as = KeepAs}}.

process(_Port, #data_point{ts = Ts, dtag = DTag, tags = Tags} = Point,
    State = #state{current = Current, fields = Fields, default = Default}) ->
   CollectedVals = flowdata:fields(Point, Fields),
   All = lists:zip(Fields, CollectedVals),
   UpdateFun =
   fun
      ({FName, undefined}, CPoint) ->
         case flowdata:field(CPoint, FName) of
            undefined ->
               case Default of
                  undefined -> CPoint;
                  _ -> flowdata:set_field(CPoint, FName, Default)
               end;
            _ -> CPoint
         end;
      ({FName1, Value1}, CPoint1) ->
         flowdata:set_field(CPoint1, FName1, Value1)
   end,
   CurrentPoint = Current#data_point{ts = Ts, dtag = DTag, tags = Tags},
   ResultPoint = lists:foldl(UpdateFun, CurrentPoint, All),
   maybe_emit(ResultPoint, keep(Point, State), State).


-spec keep(#data_point{}, #state{}) -> #data_point{}.
keep(_DataPoint, #state{keep = []}) ->
   [];
keep(DataPoint, #state{keep = FieldNames, keep_as = As0}) when is_list(FieldNames) ->
   KeepF = flowdata:fields(DataPoint, FieldNames),
   UseFieldNames =
   case As0 of
      undefined -> FieldNames;
      _ -> As0
   end,
   lists:filter(fun
                   ({_, undefined}) -> false;
                   ({_, _}) -> true
                end,
      lists:zip(UseFieldNames, KeepF)
   ).

maybe_emit(Point = #data_point{fields = Fields}, _Kept,
    State = #state{emit_unchanged = false, current = #data_point{fields = Fields}}) ->
   {ok, State#state{current = Point}};
maybe_emit(Point, Kept, State) ->
   {emit, flowdata:set_fields(Point, Kept), State#state{current = Point}}.


