%% Date: 05.01.17 - 14:11
%% Ⓒ 2017 heyoka
%% @doc
%% Evaluate a series of lambda expressions
%% @end
-module(esp_eval).
-author("Alexander Minichmair").

%% API
-behavior(df_component).

-include("faxe.hrl").
%% API
-export([init/3, process/3, options/0, check_options/0, eval/4]).


-record(state, {
   node_id,
   lambdas,
   as,
   tags
}).

options() -> [{lambdas, lambda_list}, {as, string_list}, {tags, string_list, []}].

check_options() ->
   [
      {same_length, [lambdas, as]}
   ].

init(NodeId, _Ins, #{lambdas := LambdaFuns, as := As, tags := Tags}) ->
   {ok, all, #state{lambdas = LambdaFuns, node_id = NodeId, as = As, tags = Tags}}.

process(_In, #data_batch{points = Points} = Batch,
      State = #state{lambdas = LambdaFuns, as = As, tags = Tags}) ->
   NewPoints = [eval(Point, LambdaFuns, As, Tags) || Point <- Points],
   {emit, Batch#data_batch{points = NewPoints}, State};
process(_Inport, #data_point{} = Point, State = #state{lambdas = LambdaFun, as = As, tags = Tags}) ->
%%   {T, NewValue} = timer:tc(?MODULE, eval, [Point, LambdaFun, As, Tags]),
%%   lager:info("eval took: ~p",[T]),
   NewValue = eval(Point, LambdaFun, As, Tags),
   {emit, NewValue, State}.

eval(#data_point{} = Point, Lambdas, As, _Tags) ->
   {PointResult, _Index} =
   lists:foldl(
      fun(LFun, {P, Index}) ->
         As0 = lists:nth(Index, As),
         P0 = faxe_lambda:execute(P, LFun, As0),
%%         P1 =
%%         case (catch lists:nth(Index, Tags)) of
%%            T0 when is_binary(T0) ->
%%               Po = flowdata:set_tag(P0, T0, flowdata:field(P0, As0)),
%%               flowdata:delete_field(Po, As0);
%%            _ -> P0
%%         end,
         {P0, Index + 1}

      end,
      {Point, 1},
      Lambdas
   ),
   PointResult.