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
-export([init/3, process/3, options/0]).


-record(state, {
   node_id,
   lambdas,
   as,
   tags
}).

options() -> [{lambdas, lambda_list}, {as, string_list}, {tags, string_list, []}].

init(NodeId, _Ins, #{lambdas := LambdaFuns, as := As, tags := Tags} =Ps) ->
   lager:notice("~p init:node~n~p",[NodeId, Ps]),
   {ok, all, #state{lambdas = LambdaFuns, node_id = NodeId, as = As, tags = Tags}}.

process(_In, #data_batch{points = Points} = Batch,
      State = #state{lambdas = LambdaFuns, as = As, tags = Tags}) ->
   NewPoints = [eval(Point, LambdaFuns, As, Tags) || Point <- Points],
   {emit, Batch#data_batch{points = NewPoints}, State};
process(_Inport, #data_point{} = Point, State = #state{lambdas = LambdaFun, as = As, tags = Tags}) ->
   NewValue = eval(Point, LambdaFun, As, Tags),
%%   lager:info("~p emitting: ~p",[?MODULE, NewValue]),
   {emit, NewValue, State}.

eval(#data_point{} = Point, Lambdas, As, Tags) ->
   {PointResult, _Index} =
   lists:foldl(
      fun(LFun, {P, Index}) ->
         As0 = lists:nth(Index, As),
         P0 = faxe_lambda:execute(P, LFun, As0),
         P1 =
         case (catch lists:nth(Index, Tags)) of
            T0 when is_binary(T0) ->
               lager:notice("found Tag ~p for field ~p",[T0, As0]),
               Po = flowdata:set_tag(P0, T0, flowdata:field(P0, As0)),
               lager:notice("after settag: ~p",[Po]),
               flowdata:delete_field(Po, As0);
            _ -> P0
         end,
         {P1, Index + 1}

      end,
      {Point, 1},
      Lambdas
   ),
   PointResult.