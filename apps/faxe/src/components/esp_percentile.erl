%% Date: 05.06.23- 08:02
%% Ⓒ 2023 heyoka
%%
%% @doc
%%
%% @end
-module(esp_percentile).
-author("Alexander Minichmair").

%% API
-behavior(df_component).

-include("faxe.hrl").
%% API
-export([init/3, process/3, options/0, wants/0, emits/0]).


-record(state, {
   node_id,
   fields,
   as,
   at :: pos_integer(),
  mods_paths_as = []
}).

options() -> [
   {fields, string_list},
   {as, string_list, undefined},
   {keep_last, boolean, true},
   {at, integer, 75}
].

%%check_options() ->
%%   [{one_of, mode, [?MODE_ABS, ?MODE_CP, ?MODE_PC]}].

wants() -> batch.
emits() -> point.

init(NodeId, _Ins, #{fields := Fields, as := As0, at := At}) ->
   As = case As0 of undefined -> Fields; _ -> As0 end,
    ModPathsAs = lists:zip3(lists:duplicate(length(Fields), <<"percentile">>), Fields, As),
   S = #state{node_id = NodeId, fields = Fields, as = As, at = At, mods_paths_as = ModPathsAs},
   {ok, all, S}.

process(_Inport, B=#data_batch{start = BatchStart}, State = #state{mods_paths_as = MPA, at = At}) ->
  {_FirstPoint = #data_point{ts = Ts}, _LastPoint, CalcData} = esp_aggregate:prepare_data(B, MPA, nil),
  Ts = case BatchStart of undefined -> Ts; _-> BatchStart end,
  OutPoint = do_process(MPA, CalcData, At, #data_point{ts = Ts}),
   {emit, OutPoint, State}.

do_process([], _, _, DataPoint) ->
  DataPoint;
do_process([{Mod, Path, As}|ModsPathsAs], DataMap, At, DataPoint) when is_map_key({Mod, Path}, DataMap) ->
  Data = maps:get({Mod, Path}, DataMap),
  NewValue = percentile(Data, At),
  NewPoint = flowdata:set_field(DataPoint, As, NewValue),
  do_process(ModsPathsAs, DataMap, At, NewPoint);
do_process([{_Mod, _Path, As}|ModsPathsAs], DataMap, At, DataPoint) ->
  NewDataPoint = flowdata:set_field(DataPoint, As, 0),
  do_process(ModsPathsAs, DataMap, At, NewDataPoint).

percentile(#{vals := []}, _) -> 0;
percentile(#{vals := Vals}, 0) -> lists:min(Vals);
percentile(#{vals := Vals}, 100) -> lists:max(Vals);
percentile(#{vals := List, count := Count}, N) when is_list(List) andalso is_number(N) ->
  S = lists:sort(List),
  R = N/100.0 * Count,
  F = trunc(R),
  Lower = lists:nth(F, S),
  Upper = lists:nth(F + 1, S),
  Lower + (Upper - Lower) * (R - F).

%%%%%%%%%%%%
-ifdef(TEST).



-endif.