%% Date: 9.11.20 - 20:48
%% Ⓒ 2020 heyoka
%%
%% @doc
%% With this node we can collect a unique set of values from data_points based on a given field's value.
%%
%% This node is useful, if you have multiple similar data-streams - all share a field called the key-field, who's value is unique to this data-stream -
%% that you want to condense into one data_point, according to that key-field's value;
%% sort of an "un-group" function.
%%
%% Note: This node produces a completely new data_point.
%%
%% Given a key-field-name the node collects data_points using the value of this field to group and cache every incoming data.
%% Once the 'min_vals' count of unique values is reached in the internal buffer of the node, it starts emitting every change
%% within this set of values.
%% New data_points, which have the same value for the key-field as seen before, will overwrite old values.
%% Data_points that do not have the key-field present, will be ignored.
%% On output, the node will condense the collected data_points into one, where all the data_points'
%% fields are grouped by the value of the key-field.
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
   , check_options/0
]).

-record(state, {
   node_id,
   row_buffer = #{},
   min_count,
   field
   ,keep
   ,as
}).

options() -> [
   {field, string},
   {min_vals, integer, 1}, %% number of distinct values in the buffer, before first output
   {keep, string_list}, %% a list of field path to keep for every data_point
   {as, string_list, undefined} %% rename the kept fields
].

check_options() ->
   [
      {same_length, [keep, as]}
   ].

init(NodeId, _Ins, #{field := Field, min_vals := Min, keep := Keep, as := As}) ->
   {ok, all, #state{node_id = NodeId, field = Field, min_count = Min, keep = Keep, as = As}}.

process(_Port, #data_point{} = Point, State = #state{min_count = Min, row_buffer = Buf}) ->
   NewBuffer = maybe_add_point(Point, State),
   NewState = State#state{row_buffer = NewBuffer},
   case NewBuffer /= Buf andalso maps:size(NewBuffer) >= Min of
      true ->
         P0 = #data_point{ts = faxe_time:now()},
         Fold = fun(_K, KeyValues, Point1) ->
%%            lager:notice("set keyvalues: ~p",[KeyValues]),
            flowdata:set_fields(Point1, KeyValues) end,
         P = maps:fold(Fold, P0, NewBuffer),
%%         lager:info("output buffer: ~p",[P]),
         {emit, P, NewState};
      false -> {ok, NewState}
   end.

maybe_add_point(Point, #state{row_buffer = Buffer, field = FieldName, keep = KeepFields, as = Aliases}) ->
   case flowdata:field(Point, FieldName, undefined) of
      undefined ->
         Buffer;
      Val0 ->
         Val = faxe_util:to_bin(Val0),
         Kept = keep(Point, KeepFields, Val, Aliases),
         maps:put(Val, Kept, Buffer)
   end.

keep(Point, Names, GroupName,  undefined) ->
   keep(Point, Names, GroupName, Names);
keep(Point, Names, GroupName, Aliases) ->
   Fs = flowdata:fields(Point, Names),
   Paths0 = [list_to_tuple(binary:split(Path, [<<".">>], [global, trim_all])) || Path <- Aliases],
   Paths = [erlang:insert_element(1, P, GroupName) || P <- Paths0],
   lists:zip(Paths, Fs).


