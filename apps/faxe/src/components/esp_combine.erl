%% Date: 05.01.17 - 14:11
%% Ⓒ 2017 heyoka
%%
%% @doc
%% Combine the values of 2 nodes
%%
%% Port 1 is the trigger port,
%% every time a value is received on the trigger port, the node will emit a value, combined with
%% whatever current value on port 2.
%%
%% The node will never emit on port 2 values,
%% it will only emit, when a value is received on port 1 (the trigger port).
%%
%% No output is given, if there has never arrived a value on port 2 to combine with, except when nofill is given.
%%
%% given 'fields' will be copied from the point on port 2 and added to points coming in on port 1
%% if instead param 'merge_field' is given then the two points on each port will be merge on base of that field

%% the 'fields' parameter defines the fields to inject into the combination for the stream on port 2
%% to rename these fields, parameter 'prefix' or 'aliases' can be used
%% with 'prefix_delimiter' a delimiter can be given, defaults to: '_'
%%
%%
%% @end
-module(esp_combine).
-author("Alexander Minichmair").


%% API
-behavior(df_component).

-include("faxe.hrl").
%% API
-export([init/3, process/3, options/0, inports/0, check_options/0]).

-define(PREFIX_DEL, <<"_">>).

-record(state, {
   node_id,
   fields,
   row_buffer = undefined,
   row_aliases = [],
   prefix,
   name_param,
   merge_field,
   no_fill = false
}).

inports() ->
   [
      {1, 1},
      {2, 1}
   ].

options() -> [
   {fields, string_list, undefined},
   {tags, string_list, undefined},
   {aliases, string_list, undefined},
   {prefix, binary, undefined},
   {prefix_delimiter, binary, ?PREFIX_DEL},
   {merge_field, binary, undefined},
   {nofill, is_set}
      ].

check_options() ->
   [
      {one_of_params, [fields, merge_field]}
   ].

init(NodeId, _Ins, #{fields := undefined, merge_field := MergeField, nofill := NoFill}) ->
   {ok, all, #state{node_id = NodeId, merge_field = MergeField, no_fill = NoFill}};
init(NodeId, _Ins, #{fields := Fields, aliases := Aliases, prefix := Prefix, prefix_delimiter := PFL, nofill := NoFill}) ->
   Asses =
   case Aliases of
      undefined -> Fields;
      _ -> Aliases
   end,
   NP =
      case Prefix of
         undefined -> lists:zip(Fields, Asses);
         _ when is_binary(Prefix) -> <<Prefix/binary, PFL/binary>>
      end,
   {ok, all, #state{fields = Fields, node_id = NodeId, row_aliases = Asses,
      prefix = Prefix, name_param = NP, no_fill = NoFill}}.



%% trigger port
process(1, #data_point{} = _Point, State = #state{row_buffer = undefined, no_fill = false}) ->
   {ok, State};
process(1, #data_point{} = Point, State = #state{fields = undefined, row_buffer = Buffer, merge_field = MergeField}) ->
   Merged = merge(Point, Buffer, MergeField),
   {emit, Merged, State};
process(1, #data_point{} = Point, State = #state{fields = Fs, row_buffer = Buffer, name_param = NP}) ->
   Combined = combine(Point, Buffer, Fs, NP),
   {emit, Combined, State}
;
process(2, #data_point{} = Point, State = #state{}) ->
   {ok, State#state{row_buffer = Point}};
process(_Port, #data_batch{}, State = #state{}) ->
   {ok, State}.


-spec combine(#data_point{}, undefined|#data_point{}, list(), list()|binary()) -> #data_point{}.
combine(Point=#data_point{}, undefined, _Fields, _Prefix) ->
   Point;
combine(Point=#data_point{}, SPoint=#data_point{}, Fields, Prefix) when is_binary(Prefix) ->
   N = fun(Param, FName) ->
      <<Param/binary, FName/binary>>
      end,
   trans(Point, SPoint, Fields, N, Prefix);
combine(Point=#data_point{}, SPoint=#data_point{}, Fields, Names) when is_list(Names) ->
   N = fun(Param, FName) ->
      proplists:get_value(FName, Param)
       end,
   trans(Point, SPoint, Fields, N, Names).


-spec merge(#data_point{}, undefined|#data_point{}, atom()) -> #data_point{}.
merge(P = #data_point{fields = _PointFields}, undefined, _) ->
   P;
merge(P = #data_point{fields = PointFields}, #data_point{fields = SFields}, all) ->
   NewFields = flowdata:merge(PointFields, SFields),
   P#data_point{fields = NewFields};
merge(P=#data_point{}, SPoint=#data_point{}, Field) ->
   FieldData = flowdata:field(SPoint, Field),
   PFieldData = flowdata:field(P, Field),
%%   lager:notice("merge: ~n~p with ~n~p", [PFieldData, FieldData]),
   flowdata:set_field(P, Field, flowdata:merge(PFieldData, FieldData)).


-spec trans(#data_point{}, #data_point{}, list(), function(), list()) -> #data_point{}.
trans(Point, SPoint, Fields, ToNameFun, Params) ->
   lists:foldl(
      fun(FName, P) ->
         FVal = flowdata:value(SPoint, FName),
         NName = ToNameFun(Params, FName),
         flowdata:set_field(P, NName, FVal)
      end,
      Point,
      Fields
   ).