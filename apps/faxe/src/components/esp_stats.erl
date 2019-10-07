%% Date: 05.01.17 - 14:11
%% Ⓒ 2017 heyoka
-module(esp_stats).
-author("Alexander Minichmair").

-include("faxe.hrl").
%% API
-behavior(df_component).
%% API
-export([init/3, process/3, handle_info/2, get_options/0]).

-callback execute(tuple(), term()) -> tuple().

-record(state, {
   node_id,
   field,
   factor,
   modules :: list(),
   module_state,
   as,
   row1 = [],
   row2 = [],
   row_length,
   buffer = [] :: { Ts :: non_neg_integer(), Buffer :: list(term())},
   buffer_time = 0
}).

get_options() -> [{field, binary}, {as, binary, undefined}, {func, atomlist, undefined}].


init(NodeId, Ins, #{field := Field, as := As, module := Mod, func := Func} = Args) ->
   lager:notice("ARgs for ~p: ~p", [Mod, Args]),
   RowLength = length(Ins),
   Modules = case Func of undefined -> [Mod]; _ -> Func end,
   As1 = case As of undefined -> Mod; _ -> As end,
   State = #state{field = Field, node_id = NodeId, as = As1, row_length = RowLength, modules = Modules},
   {ok, all, State#state{module_state = Args}}.

process(_Inport, #data_batch{} = Batch, State = #state{modules = Mod, module_state = MState, field = F, as = As}) ->

   Ps = prepare(Batch, F),
   Result = call(Ps, Mod, MState, As),
   lager:info("~p emitting: ~p",[Mod, Result]),
   {emit, Result, State}
;
process(_Inport, #data_point{ts = Ts_Buffer} = Point,
    State = #state{field = F, buffer_time = Ts_Buffer}) ->
   State#state{buffer = [{Ts_Buffer, flowdata:field(Point, F)}|State#state.buffer]};
process(_Inport, #data_point{ts = Ts} = Point,
    State = #state{as = As, modules = Mod, module_state = MState, field = F}) ->
   lager:info("~p buffertime: ~p", [?MODULE, State#state.buffer_time]),
   NewState =
      case Ts > State#state.buffer_time of
         true ->
            %% do not emit uninitialized
            case State#state.buffer_time == 0 of
               false ->
                  TsVal = lists:unzip(State#state.buffer),
                  Result = call(TsVal, Mod, MState, As),
                  lager:info("~p emitting: ~p", [Mod, Result]),
                  dataflow:emit(Result);
               true -> ok
            end,
            %% reset buffer
            State#state{buffer = [{Ts, flowdata:field(Point, F)}], buffer_time = Ts};
         false -> lager:info("~p TS < BufferTime", [?MODULE]), State
      end,
   lager:info("~p buffer: ~p", [?MODULE, NewState#state.buffer]),
   {ok, NewState}.

handle_info(Request, State) ->
   io:format("~p request: ~p~n", [State, Request]),
   {ok, State}.


%%%%%%%%%%%%%%%%%%%% internal %%%%%%%%%%%%

%% prepare a data_batch for aggregate execution
prepare(B=#data_batch{}, Field) ->
   {flowdata:ts(B), flowdata:field(B, Field)}.

-spec call(tuple(), atom(), term(), binary()) -> #data_batch{} | #data_point{}.
call({Tss,_Vals}=Data, Module, MState, As) when is_list(Tss) ->
   c_agg:call(Data, Module, MState, As).
