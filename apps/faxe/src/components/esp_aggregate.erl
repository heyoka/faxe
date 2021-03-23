%% Date: 09.01.20 - 10:48
%% Ⓒ 2020 TGW-Group
-module(esp_aggregate).
-author("Alexander Minichmair").

-include("faxe.hrl").
%% API

-behavior(df_component).
%% API
-export([init/3, process/3, handle_info/2, options/0, check_options/0]).

-callback execute(tuple(), term()) -> tuple().

-record(state, {
   node_id,
   fields,
   factor,
   modules :: list(),
   module_state,
   as,
   row_length
}).

options() -> [
   {fields, binary_list},
   {as, binary_list},
   {modules, binary_list}
].

check_options() ->
   [
      {same_length, [fields, as, modules]},
      {one_of, modules,
         [<<"variance">>, <<"sum">>, <<"min">>, <<"max">>, <<"stddev">>,
            <<"first">>, <<"last">>, <<"avg">>, <<"count">>, <<"count_distinct">>,
            <<"mean">>, <<"median">>, <<"range">>, <<"skew">>]}
   ].

init(NodeId, Ins, #{fields := Fields, as := As, modules := Mods} = Args) ->
   RowLength = length(Ins),
   Modules = [binary_to_atom(<<"esp_", Mod/binary>>, latin1) || Mod <- Mods],
   State = #state{fields = Fields, node_id = NodeId, as = As, row_length = RowLength, modules = Modules},
   {ok, all, State#state{module_state = Args}}.


%%% databatch only
process(_Inport, #data_batch{} = Batch, State = #state{modules = Mods, module_state = MState, fields = Fields, as = As}) ->

   Ps = [prepare(Batch, F) || F <- Fields],
   MStates = [MState || _F <- Fields],

   {Ts, Results} = call(Ps, Mods, MStates, As, {0, []}),
   NewPoint = flowdata:set_fields(#data_point{ts = Ts}, Results),
   {emit, NewPoint, State};
process(_, #data_point{}, _State) ->
   {error, datapoint_not_supported}.

handle_info(Request, State) ->
   lager:warning("~p request: ~p~n", [State, Request]),
   {ok, State}.


%%%%%%%%%%%%%%%%%%%% internal %%%%%%%%%%%%

%% prepare a data_batch for aggregate execution
prepare(B=#data_batch{}, Field) ->
   {flowdata:ts(B), flowdata:field(B, Field)}.

call([], [], [], [], Results) ->
   Results;
call([Point|Points], [Mod|Modules], [MState|MStates], [As|Aliases], {_Ts, Acc}) ->
   Res = call(Point, Mod, MState, As),
   Result = {As, flowdata:field(Res, As)},
   call(Points, Modules, MStates, Aliases, {flowdata:ts(Res), [Result]++Acc}).

-spec call(tuple(), atom(), term(), binary()) -> #data_batch{} | #data_point{}.
call({Tss,_Vals}=Data, Module, MState, As) when is_list(Tss) ->
   c_agg:call(Data, Module, MState, As).
