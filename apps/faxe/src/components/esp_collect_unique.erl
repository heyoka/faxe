%% Date: 9.11.20 - 20:48
%% Ⓒ 2020 heyoka
%%
%% @doc
%% With this node we can collect a unique set of values from data_points based on a given field's value.
%%
%% This node is useful, if you have multiple similar data-streams - all share a field, that is unique to this data-stream -
%% that you want to condense into one, according to that field's value;
%% sort of an "un-group" function.
%%
%% Note: This node produces a completely new data_point
%% Given a field-name the node collects data_points using the value of this field to group and cache every incoming data.
%% Once the 'min_vals' count of unique values is reached in the internal buffer of the node, it starts emitting every change
%% within this set of values.
%% On output, the node will condense the collected data_points into one, where all the data_points' fields are grouped by the value
%% of the field that is given.
%% Note: The number of uniquely collected values will grow, but never shrink (at the moment).

%% Also note: The produced data_point can become very large, if the value of the specified field is ever changing, so that
%% the node will cache a lot of data and therefore may use a lot of memory, be aware of that !
%%
%%
%% @end
-module(esp_collect_unique).
-author("Alexander Minichmair").


%% API
-behavior(df_component).

-include("faxe.hrl").
%% API
-export([init/3, process/3, options/0
%%   , check_options/0
]).

-record(state, {
   node_id,
   row_buffer = #{},
   min_count,
   field
   ,emit
   ,keep
}).

options() -> [
   {field, binary},
   {min_vals, integer, 2}, %% number of distinct values in the buffer, before first output
   {emit, string, <<"every">>}, %% every, fill,
   {keep, string_list, undefined}
].

init(NodeId, _Ins, #{field := Field, emit = Emit, min_vals := Min}) ->
   Buffer = sets:new(),
   {ok, all, #state{node_id = NodeId, field = Field, emit = Emit, min_count = Min, row_buffer = Buffer}}.

process(_Port, #data_point{} = Point, State = #state{min_count = Min}) ->
   NewBuffer = maybe_add_point(Point, State),
   case length(NewBuffer) >= Min of
      true -> {emit, #data_point{fields = NewBuffer, ts = faxe_time:now()}, State};
      false -> {ok, State#state{row_buffer = NewBuffer}}
   end.

maybe_add_point(Point#{}, #state{row_buffer = Buffer, field = FieldName, keep = FieldNames}) ->
   case flowdata:field(Point, FieldName, undefined) of
      undefined -> Buffer;
      Val -> Fields = keep(Point, FieldNames), maps:put(Val, Fields, Buffer)
   end.

keep(#data_point{fields = Fields}, undefined) ->
   Fields;
keep(Point, Names) ->
   flowdata:fields(Point, Names).

