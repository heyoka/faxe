%% Date: 05.06.23- 08:02
%% Ⓒ 2023 heyoka
%%
%% @doc
%%
%% @end
-module(esp_count_change).
-author("Alexander Minichmair").

%% API
-behavior(df_component).

-include("faxe.hrl").
%% API
-export([init/3, process/3, options/0, wants/0, emits/0]).


-record(state, {
   node_id,
   field,
   as,
   keep_last = true,
   last = nil
}).

options() -> [
   {field, string, undefined},
   {as, string, undefined},
   {keep_last, boolean, true}
].

%%check_options() ->
%%   [{one_of, mode, [?MODE_ABS, ?MODE_CP, ?MODE_PC]}].

wants() -> batch.
emits() -> point.

init(NodeId, _Ins, #{field := Field, as := As, keep_last := KeepLast}) ->
   LastP = flowdata:set_field(flowdata:new(), Field, nil),
   S = #state{node_id = NodeId, field = Field, as = As, keep_last = KeepLast, last = LastP},
   {ok, all, S}.

process(_Inport, #data_batch{points = Points, start = BatchStart}, State = #state{field = Field, as = As, last = Last}) ->
   {FirstPoint, LastPoint, Count} = do_process(Field, Last, Points),
   NewLast =
   case State#state.keep_last of
      true -> LastPoint;
      false -> Last
   end,
   Ts = case BatchStart of undefined -> FirstPoint#data_point.ts; _-> BatchStart end,
   OutPoint0 = #data_point{ts = Ts},
   OutPoint = flowdata:set_field(OutPoint0, As, Count),
   {emit, OutPoint, State#state{last = NewLast}}.

do_process(Field, LastP = #data_point{}, Points) ->
   F = fun
          (P, {FirstPoint, LastPoint, Count}) ->
             NewFirst =
             case FirstPoint of undefined -> P; _ -> FirstPoint end,
             NewCount =
             case flowdata:field(P, Field) =:= flowdata:field(LastPoint, Field) of
                true -> Count;
                _ -> Count+1
             end,
             {NewFirst, P, NewCount}
       end,
   lists:foldl(F, {undefined, LastP, 0}, Points).
%%%%%%%%%%%%
-ifdef(TEST).

-endif.