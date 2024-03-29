%% Date: 06.06.23 - 10:02
%% Ⓒ 2023
-module(esp_aggregate).
-author("Alexander Minichmair").

-include("faxe.hrl").
%% API

-behavior(df_component).
%% API
-export([init/3, process/3, handle_info/2, options/0, check_options/0, wants/0, emits/0]).
-export([prepare_data/3, prepare_each/4, get_path_value/2]).

-define(FUNCTIONS,
   [
      <<"variance">>,
      <<"sum">>,
      <<"min">>,
      <<"max">>,
      <<"stddev">>,
%%      <<"first">>,
%%      <<"last">>,
      <<"avg">>,
      <<"count">>,
      <<"count_distinct">>,
      <<"count_change">>,
      <<"mean">>,
      <<"geometric_mean">>,
      <<"median">>,
      <<"range">>,
      <<"skew">>
   ]
).

-record(state, {
   node_id :: binary(),
   module_states,
   as :: binary(),
   keep :: list(),
   default_ts :: non_neg_integer(),
   last_point = undefined :: undefined | #data_point{},
   mod_paths_as = [],
   keep_tail = true
}).

options() -> [
   {fields, string_list},
   {as, string_list, undefined},
   {functions, string_list},
   {keep, string_list, []},
   {keep_tail, boolean, true}
].

check_options() ->
   [
      {same_length, [fields, as, functions]},
      {one_of, functions, ?FUNCTIONS}
   ].

wants() -> batch.
emits() -> point.

init(NodeId, _Ins, #{fields := Fields, functions := Funcs, keep := Keep, keep_tail := KeepTail} = Args) ->

   As = init_as(Args),
   MStates = [Args || _F <- Fields],

   State = #state{
      node_id = NodeId, as = As, keep = Keep,
      mod_paths_as = lists:zip3(Funcs, Fields, As),
      keep_tail = KeepTail},
   {ok, all, State#state{module_states = MStates}}.

init_as(#{as := undefined, fields := Fields, functions := Funs}) ->
   [<<Field/binary, "_", Fun/binary>> || {Field, Fun} <- lists:zip(Fields, Funs)];
init_as(#{as := As}) ->
   As.

%%% databatch only
process(_Inport, #data_batch{points = [], start = BatchStart}, State = #state{as = Aliases}) ->
   %% the only chance for us to get a timestamp here, is to take the batchstart or else use now() ;(
   Ts = case BatchStart of
           undefined -> faxe_time:now();
           _ when is_integer(BatchStart) -> BatchStart
        end,
   NewFields = [{As, 0} || As <- Aliases],
   NewPoint = flowdata:set_fields(#data_point{ts = Ts}, NewFields),
   %% emit zeroed fields
   {emit, NewPoint, State};
process(_Inport, #data_batch{start = BatchStart} = Batch,
    State = #state{mod_paths_as = ModPathsAs, last_point = LastPoint, keep = KeepFields, keep_tail = KeepLast}) ->
   {FirstPoint = #data_point{ts = Ts}, NewLastPoint, CalcData} =
      prepare_data(Batch, ModPathsAs, LastPoint),
%%   lager:warning("prepared data ~p ~n LASTPOINT:~p",[CalcData, State#state.last_point]),
   Results = do_process(ModPathsAs, CalcData, []),
   TsBatch =
      case BatchStart of
         undefined -> Ts;
         _ when is_integer(BatchStart) -> BatchStart
      end,
%% handle keep fields
   KeepKV = lists:zip(KeepFields, flowdata:fields(FirstPoint, KeepFields)),
   NewPoint = flowdata:set_fields(#data_point{ts = TsBatch}, KeepKV++Results),
%%   NewPoint = flowdata:set_fields(NewPoint0, Results),
%% emit
   Last = case KeepLast of true -> NewLastPoint; false -> nil end,
   {emit, NewPoint, State#state{last_point = Last}};

process(_, #data_point{}, _State) ->
   {error, datapoint_not_supported}.

handle_info(_Request, State) ->
   {ok, State}.

%%%%%%%%%%%%%%%%%%%% internal %%%%%%%%%%%%

prepare_data(#data_batch{points = Points}, ModPathsAs, LastPoint) when is_list(ModPathsAs) ->
   lists:foldl(
      fun
         (#data_point{}=P, {FirstPoint0, _LastPoint, FieldsVals}) ->
            FirstPoint = case FirstPoint0 of undefined -> P; _ -> FirstPoint0 end,
            Data = prepare_each(P, ModPathsAs, FieldsVals, LastPoint),
            {FirstPoint, P, Data}
      end,
      {undefined, undefined, #{}},
      Points
   ).

prepare_each(P = #data_point{}, ModPathsAs, AccMap, LastPoint) ->
   lists:foldl(
      fun
         ({Mod, Path, _As}, InnerValMap) ->
            Key = {Mod, Path},
            Val = get_path_value(Path, P),
            case Val of
               undefined ->
                  InnerValMap;
               _ ->
                  FEntry = #{count := PCount} =
                     case maps:get(Key, InnerValMap, undefined) of
                        undefined -> #{count => 0, path => Path};
                        FCurrent -> FCurrent
                     end,
%%                  lager:info("prepare_mode(~p, ~p, ~p, ~p)",[Mod, FEntry, Val, LastPoint]),
                  VMap = prepare_mod(Mod, FEntry, Val, LastPoint),
                  InnerValMap#{Key => VMap#{count => PCount+1}}
            end
      end,
      AccMap,
      ModPathsAs).

get_path_value(Path, Point=#data_point{}) ->
   flowdata:field(Point, Path);
get_path_value(_Path, _Point) ->
   undefined.

do_process([], _, Acc) ->
   Acc;
do_process([{Mod, Path, As}|ModsPathsAs], DataMap, Acc) when is_map_key({Mod, Path}, DataMap) ->
   Data = maps:get({Mod, Path}, DataMap),
   NewValue = agg(Mod, Data),
   NewAcc = [{As, NewValue}|Acc],
   do_process(ModsPathsAs, DataMap, NewAcc);
do_process([{_Mod, _Path, As}|ModsPathsAs], DataMap, Acc) ->
   NewAcc = [{As, 0}|Acc],
   do_process(ModsPathsAs, DataMap, NewAcc).


%% prepare for each aggregation function
prepare_mod(<<"count">>, Data, _NewVal, _LP) ->
   Data;
prepare_mod(<<"sum">>, Data = #{sum := Sum}, NewVal, _LP) ->
   Data#{sum => Sum + NewVal};
prepare_mod(<<"sum">>, Data = #{}, NewVal, _LP) ->
   Data#{sum => NewVal};
prepare_mod(<<"min">>, Data = #{min := CurrentMin}, NewVal, _LP) ->
   Data#{min => min(CurrentMin, NewVal)};
prepare_mod(<<"min">>, Data = #{}, NewVal, _LP) ->
   Data#{min => NewVal};
prepare_mod(<<"max">>, Data = #{max := CurrentMin}, NewVal, _LP) ->
   Data#{max => max(CurrentMin, NewVal)};
prepare_mod(<<"max">>, Data = #{}, NewVal, _LP) ->
   Data#{max => NewVal};
prepare_mod(<<"avg">>, Data, NewVal, LP) ->
   prepare_mod(<<"sum">>, Data, NewVal, LP);
prepare_mod(<<"mean">>, Data, NewVal, LP) ->
   prepare_mod(<<"sum">>, Data, NewVal, LP);
prepare_mod(<<"range">>, Data = #{min := Min, max := Max}, NewVal, _LP) ->
   Data#{min => min(Min, NewVal), max => max(Max, NewVal)};
prepare_mod(<<"range">>, Data, NewVal, _LP) ->
   Data#{min => NewVal, max => NewVal};

prepare_mod(<<"count_change">>, D = #{last := Value}, Value, _LP) ->
   D;
prepare_mod(<<"count_change">>, #{change_count := CurrentCount}, NewVal, _LP) ->
   #{change_count => CurrentCount+1, last => NewVal};
prepare_mod(<<"count_change">>, #{path := Path}, NewVal, #data_point{} = P) ->
   LastVal = flowdata:field(P, Path, nil),
   InitCount =
   case LastVal =:= NewVal of
      true -> 0;
      false -> 1
   end,
   #{change_count => InitCount, last => NewVal};
prepare_mod(<<"count_change">>, #{}, NewVal, _LastP) ->
   #{change_count => 1, last => NewVal};

%% for all other mods, we collect the values in a list
prepare_mod(_, Data = #{vals := Vals}, NewVal, _LP) ->
   Data#{vals => [NewVal|Vals]};
prepare_mod(_, Data, NewVal, _LP) ->
   Data#{vals => [NewVal]}.

%% actual aggregation
agg(<<"count">>, #{count := Count}) ->
   Count;
agg(<<"sum">>, #{sum := Sum}) ->
   Sum;
agg(<<"min">>, #{min := Min}) ->
   Min;
agg(<<"max">>, #{max := Max}) ->
   Max;
agg(<<"avg">>, #{sum := Sum, count := Count}) ->
   Sum/Count;
agg(<<"mean">>, Data) ->
   agg(<<"avg">>, Data);
agg(<<"geometric_mean">>, #{count := Count, vals := Values}) ->
   [First|Values1] = Values,
   Prod = lists:foldl(
      fun(E, Acc) -> E * Acc end,
      First, Values1
   ),
   mathex:nth_root(Count, Prod);
agg(<<"median">>, #{vals := Vals, count := Count}) ->
   Sorted = lists:sort(Vals),
   case (Count rem 2) == 0 of
      false ->
         lists:nth(trunc((Count+1)/2), Sorted);
      true ->
         Nth = trunc(Count/2),
         (lists:nth(Nth, Sorted) + lists:nth(Nth+1, Sorted)) / 2
   end;
agg(<<"variance">>, #{vals := Vals}) ->
   mathex:variance(Vals);
agg(<<"stddev">>, #{vals := Vals}) ->
   mathex:stdev_sample(Vals);
agg(<<"count_distinct">>, #{vals := Vals}) ->
   sets:size(sets:from_list(Vals, [{version, 2}]));
agg(<<"count_change">>, #{change_count := Count}) ->
   Count;
agg(<<"range">>, #{min := Min, max := Max}) ->
   Max-Min;
agg(<<"skew">>, #{vals := Vals}) ->
   mathex:skew(Vals);
agg(What, _) ->
   lager:error("aggregation type ~p not implemented or no values", [What]),
   0.

