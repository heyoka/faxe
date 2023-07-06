%% Date: 06.06.23 - 20:11
%% Ⓒ 2023 heyoka
%% @doc
%% given a data_point with 1 or more array fields, create and emit 1 new data_point for every entry in the array(s)
%% if more than 1 array is used, they have to be the same size, otherwise the mapping will fail
%% for every point created a time offset will be added to the previous point timestamp, starting with the timestamp of the
%% original data_point
%% @end
-module(esp_array_explode).
-author("Alexander Minichmair").

%% API
-behavior(df_component).

-include("faxe.hrl").
%% API
-export([init/3, process/3, options/0, check_options/0, wants/0, emits/0]).


-record(state, {
   node_id,
   fields,
   as,
   offset = 0,
   keep
}).

options() -> [
   {fields, string_list},
   {as, string_list, undefined},
   {time_offset, duration, <<"1s">>},
   {keep, string_list, undefined}
   ].


check_options() ->
   [
      {same_length, [fields, as]}
   ].

wants() -> point.
emits() -> point.

init(NodeId, _Ins, #{fields := Fields, as := As0, time_offset := Offset0, keep := Keep}) ->
   Offset = faxe_time:duration_to_ms(Offset0),
   As = case As0 of undefined -> Fields; _ -> As0 end,
   {ok, all, #state{fields = Fields, node_id = NodeId, as = As, offset = Offset, keep = Keep}}.

process(_Inport, #data_point{ts = TsStart} = Point, State = #state{fields = Fields, as = As, offset = Offset}) ->
   DataList0 = lists:zip(As, flowdata:fields(Point, Fields)),
   %% filter non existing fields
   [{_Alias, List}|_R] = DataList = [{A, DEntry} || {A, DEntry} <- DataList0, DEntry /= undefined],
   OutPort = 1,
   %% list of values to keep for the current point
   KeepList = keep_list(Point, State#state.keep),
   {_,InitPoints} = lists:foldl(
      fun(_, {TimeStamp, PList}) ->
         {TimeStamp + Offset, PList ++ [{OutPort, flowdata:set_fields(#data_point{ts = TimeStamp}, KeepList)}]}
      end, {TsStart, []}, List),

   Out = map_data(DataList, InitPoints),
   {emit, Out, State}.

map_data([], PointsAcc) ->
   PointsAcc;
map_data([{AsName, DList}|DataList], PointsAcc) ->
   NewAcc =
   lists:map(
      fun({Val, {Port, Point}}) ->
         {Port, flowdata:set_field(Point, AsName, Val)}
      end,
      lists:zip(DList, PointsAcc)),
   map_data(DataList, NewAcc).

keep_list(_P, undefined) ->
   [];
keep_list(P, Fields) when is_list(Fields) ->
   lists:zip(Fields, flowdata:fields(P, Fields)).






