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
   , wants/0, emits/0, init/4]).

-record(state, {
   node_id,
   row_buffer = #{},
   min_count,
   field
   ,keep
   ,keep_as,
   as,
   max_age
}).

options() -> [
   {field, string},
   {min_vals, integer, 1}, %% number of distinct values in the buffer, before first output
   {keep, string_list}, %% a list of field path to keep for every data_point
   {keep_as, string_list, undefined}, %% rename the kept fields
   {as, string, undefined}, %% rename the whole field construct on output
   {max_age, duration, undefined}
].

check_options() ->
   [
      {same_length, [keep, keep_as]}
   ].

wants() -> point.
emits() -> point.

%% init with persistent state
init(NodeId, Ins, Opts, #node_state{state = #{row_buffer := StateBuffer}}) ->
   {ok, W, InitState} = init(NodeId, Ins, Opts),
   {ok, W, InitState#state{row_buffer = StateBuffer}}.

init(NodeId, _Ins, #{field := Field, min_vals := Min, keep := Keep, keep_as := KeepAs, as := As, max_age := MaxAge0}) ->
   MaxAge = case MaxAge0 of undefined -> undefined; Age -> faxe_time:duration_to_ms(Age) end,
   {ok, all,
      #state{node_id = NodeId, field = Field, min_count = Min, keep = Keep, keep_as = KeepAs, as = As, max_age = MaxAge}}.

process(_Port, #data_point{} = Point, State = #state{min_count = Min, row_buffer = Buf, as = As, node_id = NId}) ->
   NewBuffer = maybe_add_point(Point, State),
   %% maybe persist state
   case Buf /= NewBuffer of
      true -> dataflow:persist(NId, #{row_buffer => NewBuffer});
      false -> ok
   end,
   NewState = State#state{row_buffer = NewBuffer},
   case NewBuffer /= Buf andalso maps:size(NewBuffer) >= Min of
      true ->
         P0 = #data_point{ts = faxe_time:now()},
         Fold = fun(_K, #{data := KeyValues}, Point1) ->
%%            lager:notice("set keyvalues: ~p",[KeyValues]),
            flowdata:set_fields(Point1, KeyValues) end,
         P = maps:fold(Fold, P0, NewBuffer),
%%         lager:info("output point: ~p",[maybe_rewrite(P, As)]),
         {emit, maybe_rewrite(P, As), NewState};
      false -> {ok, NewState}
   end.

maybe_rewrite(P, undefined) ->
   P;
maybe_rewrite(P = #data_point{fields = Fields}, Path) ->
   NewFields = jsn:set(flowdata:path(Path), #{}, Fields),
   P#data_point{fields = NewFields}.

maybe_add_point(Point = #data_point{ts = Ts},
    State = #state{row_buffer = Buffer0, field = FieldName, keep = KeepFields, keep_as = Aliases}) ->
   Buffer = clean(Buffer0, State),
%%   lager:info("Buffer: ~p", [Buffer]),
   case flowdata:field(Point, FieldName, undefined) of
      undefined ->
         Buffer;
      Val0 ->
         Val = faxe_util:to_bin(Val0),
         Kept = keep(Point, KeepFields, Val, Aliases),
         PointData = #{data => Kept, ts => Ts},
         maps:put(Val, PointData, Buffer)
   end.

keep(Point, Names, GroupName,  undefined) ->
   keep(Point, Names, GroupName, Names);
keep(Point, Names, GroupName, Aliases) ->
   Fs = flowdata:fields(Point, Names),
   Paths0 = [list_to_tuple(binary:split(Path, [<<".">>], [global, trim_all])) || Path <- Aliases],
   Paths = [erlang:insert_element(1, P, GroupName) || P <- Paths0],
   lists:zip(Paths, Fs).

clean(Buffer, #state{max_age = undefined}) ->
   Buffer;
clean(Buffer, #state{max_age = MaxAge}) ->
%%   F = fun(_Key, #{ts := Ts}) -> lager:notice("ts too old?:~p",[Ts > faxe_time:now() - MaxAge]) end,
%%   maps:map(F, Buffer),
   Pred = fun(_Key, #{ts := Ts}) -> Ts > faxe_time:now() - MaxAge end,
   maps:filter(Pred, Buffer).
