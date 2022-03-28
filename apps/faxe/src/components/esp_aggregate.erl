%% Date: 09.01.21 - 10:02
%% Ⓒ 2021 TGW-Group
-module(esp_aggregate).
-author("Alexander Minichmair").

-include("faxe.hrl").
%% API

-behavior(df_component).
%% API
-export([init/3, process/3, handle_info/2, options/0, check_options/0, wants/0, emits/0]).

-define(FUNCTIONS,
   [
      <<"variance">>,
      <<"sum">>,
      <<"min">>,
      <<"max">>,
      <<"stddev">>,
      <<"first">>,
      <<"last">>,
      <<"avg">>,
      <<"count">>,
      <<"count_distinct">>,
      <<"count_change">>,
      <<"mean">>,
      <<"median">>,
      <<"range">>,
      <<"skew">>
   ]
).

-record(state, {
   node_id :: binary(),
   fields :: list(),
   modules :: list(),
   module_state,
   as :: binary(),
   keep :: list(),
   default_ts :: non_neg_integer()
}).

options() -> [
   {fields, string_list},
   {as, string_list, undefined},
   {functions, string_list},
   {keep, string_list, []}
].

check_options() ->
   [
      {same_length, [fields, as, functions]},
      {one_of, functions, ?FUNCTIONS}
   ].

wants() -> batch.
emits() -> point.

init(NodeId, _Ins, #{fields := Fields, functions := Mods, keep := Keep} = Args) ->
   Modules = [binary_to_atom(<<"esp_", Mod/binary>>, latin1) || Mod <- Mods],
   As = init_as(Args),
   State = #state{fields = Fields, node_id = NodeId, as = As, keep = Keep, modules = Modules},
   {ok, all, State#state{module_state = Args}}.

init_as(#{as := undefined, fields := Fields, functions := Funs}) ->
   [<<Field/binary, "_", Fun/binary>> || {Field, Fun} <- lists:zip(Fields, Funs)];
init_as(#{as := As}) ->
   As.

%%% databatch only
process(_Inport, #data_batch{points = []}, S) ->
   {ok, S};
process(_Inport, #data_batch{points = Points, start = BatchStart} = Batch,
    State = #state{modules = Mods, module_state = MState, fields = Fields, as = As, keep = KeepFields}) ->

   DefaultTs = faxe_time:now(),
   Ps = [flowdata:tss_fields(Batch, F) || F <- Fields],
   MStates = [MState || _F <- Fields],

   KeepPoint = lists:last(Points),

   %% if there is no timestamp present in the result, we use the ts field form the last point from incoming data_batch
   {Ts, Results} =
   case call(Ps, Mods, MStates, As, {0, []}, DefaultTs) of
      {Ts1, Results1} when is_integer(Ts1) -> {Ts1, Results1};
      {_, Results2} -> {DefaultTs, Results2}
   end,
   TsBatch =
   case BatchStart of
      undefined -> Ts;
      _ when is_integer(BatchStart) -> BatchStart
   end,
   %% handle keep fields
   KeepKV = lists:zip(KeepFields, flowdata:fields(KeepPoint, KeepFields)),
   NewPoint0 = flowdata:set_fields(#data_point{ts = TsBatch}, KeepKV),
   NewPoint = flowdata:set_fields(NewPoint0, Results),
   %% emit
   {emit, NewPoint, State};
process(_, #data_point{}, _State) ->
   {error, datapoint_not_supported}.

handle_info(_Request, State) ->
   {ok, State}.

%%%%%%%%%%%%%%%%%%%% internal %%%%%%%%%%%%

call([], [], [], [], Results, _DefaultTs) ->
   Results;
call([Point|Points], [Mod|Modules], [MState|MStates], [As|Aliases], {_Ts, Acc}, DefaultTs) ->
   Res = call(Point, Mod, MState, As),
   ResVal =
   case flowdata:field(Res, As, 0) of
      [] -> 0;
      Val -> Val
   end,
   Result = {As, ResVal},
   call(Points, Modules, MStates, Aliases, {flowdata:ts(Res, DefaultTs), [Result]++Acc}, DefaultTs).

-spec call(tuple(), atom(), term(), binary()) -> #data_batch{} | #data_point{}.
call({Tss,_Vals}=Data, Module, MState, As) when is_list(Tss) ->
   c_agg:call(Data, Module, MState, As).
