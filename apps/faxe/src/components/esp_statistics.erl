%% Date: 05.01.17 - 14:11
%% Ⓒ 2017 heyoka
-module(esp_statistics).
-author("Alexander Minichmair").

-include("faxe.hrl").
%% API

-behavior(df_component).
%% API
-export([init/3, process/3, options/0]).

-callback execute(tuple(), term()) -> tuple().

-record(state, {
   node_id,
   field,
   modules :: list(),
   module_state,
   as,
   mods_as
}).

options() -> [
   {field, binary},
   {fields, string_list},
   {as, string_list, undefined},
   {modules, string_list}
].


init(NodeId, _Ins, #{field := Field, as := As, modules := Funcs} = Args) ->
   Modules = [binary_to_existing_atom(<<"esp_", M/binary>>, latin1) || M <- Funcs],
   As1 = case As of undefined -> Funcs; _ -> As end,
   State = #state{field = Field, node_id = NodeId, as = As1, modules = Modules},
   {ok, all, State#state{module_state = Args, mods_as = lists:zip(Modules, As1)}}.

process(_Inport, #data_batch{} = Batch, State = #state{mods_as = Mods_As, module_state = MState, field = F}) ->

   Ps = prepare(Batch, F),
   Points = [call(Ps, Mod, MState, As) || {Mod, As} <- Mods_As],
   MPoint = flowdata:merge_points(Points),
   {emit, MPoint, State}
.

%%%%%%%%%%%%%%%%%%%% internal %%%%%%%%%%%%

%% prepare a data_batch for aggregate execution
prepare(B=#data_batch{}, Field) ->
   {flowdata:ts(B), flowdata:field(B, Field)}.

-spec call(tuple(), atom(), term(), binary()) -> #data_batch{} | #data_point{}.
call({Tss,_Vals}=Data, Module, MState, As) when is_list(Tss) ->
   c_agg:call(Data, Module, MState, As).
