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
   default
}).

options() -> [
   {fields, string_list},
   {default, any, undefined}
].

check_options() ->
   [
%%      {same_length, [keep, keep_as]}
   ].

wants() -> point.
emits() -> point.

init(NodeId, _Ins, #{fields := Fields, default := Default}) ->
   {ok, all,
      #state{node_id = NodeId, fields = Fields, default = Default}}.

process(_Port, #data_point{ts = Ts, dtag = DTag, tags = Tags} = Point,
    State = #state{current = Current, fields = Fields, default = Default}) ->
   CollectedVals = flowdata:fields(Point, Fields),
   All = lists:zip(Fields, CollectedVals),
   lager:notice("all vals: ~p",[All]),
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
   {emit, ResultPoint, State#state{current = ResultPoint}}.


