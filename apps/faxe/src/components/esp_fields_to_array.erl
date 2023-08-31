%% Date: 2023-04-06 20:10
%% Ⓒ 2023 heyoka
%%
%% @doc
%% for every given field in 'fields', holds the last value seen for this field
%% output a data-point with all the fields currently in the buffer on every incoming data-item
%% it is possible to set a default value for fields that have not be seen so far
%% timestamp, delivery_tag and all tags of the current incoming data-point will be used in the output point
%% @end
-module(esp_fields_to_array).
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
   key_name,
   value_name,
   as,
   keep
}).

options() -> [
   {fields, string_list},
   {key_name, string},
   {value_name, string},
   {ts_as, string, undefined},
   {keep, string_list, undefined},
   {as, string}
].

check_options() ->
   [
%%      {same_length, [fields, ke]}
   ].

wants() -> point.
emits() -> point.

init(NodeId, _Ins, #{fields := Fields, key_name := KeyName, value_name := ValueName, as := As, keep := Keepers}) ->
   {ok, all,
      #state{node_id = NodeId, fields = Fields, key_name = KeyName, value_name = ValueName, as = As, keep = Keepers}}.

process(_Port, #data_point{} = Point,
    State = #state{fields = Fields, key_name = KeyName, value_name = ValName, as = As, keep = Keepers}) ->
   %% get all fields
   AllFields0 = lists:zip(Fields, flowdata:fields(Point, Fields)),
   KeepFields = maps:from_list(lists:zip(Keepers, flowdata:fields(Point, Keepers))),
   F = fun({FName, Val}) ->
      M0 = #{KeyName => binary:replace(FName, <<"*">>, <<".">>), ValName => Val},
%%      flowdata:merge(),
      maps:merge(M0, KeepFields)
      end,
   AllFields = lists:map(F, AllFields0),
   Result = flowdata:set_field(Point, As, AllFields),
%%   lager:notice("All Fields ~p",[AllFields]),
%%   {ok, State}.
   {emit, Result, State#state{}}.


