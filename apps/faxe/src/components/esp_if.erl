%% Date: 27.03.23 - 14:11
%% Ⓒ 2023 heyoka
%% @doc
%% if then else node
%% @end
-module(esp_if).
-author("Alexander Minichmair").

%% API
-behavior(df_component).

-include("faxe.hrl").
%% API
-export([init/3, process/3, options/0, check_options/0]).


-record(state, {
   node_id,
   if_lambda,
   true_expression,
   false_expression,
   as
}).

options() -> [
   {test, lambda},
   {then, any},
   {else, any, undefined},
   {as, string}].

check_options() ->
   [
%%      {same_length, [lambdas, as]}
   ].

init(NodeId, _Ins, #{test := IfLambda, as := As, then := True, else := False}) ->
   {ok, all, #state{if_lambda = IfLambda, node_id = NodeId, as = As, true_expression = True, false_expression = False}}.

process(_In, #data_batch{points = Points} = Batch, State = #state{}) ->
   NewPoints = [process_point(Point, State) || Point <- Points],
   {emit, Batch#data_batch{points = NewPoints}, State};
process(_Inport, #data_point{} = Point, State = #state{}) ->
   NewPoint = process_point(Point, State),
   {emit, NewPoint, State}.

process_point(Point, #state{if_lambda = FunIf, true_expression = True, false_expression = False, as = As}) ->
   case faxe_lambda:execute_bool(Point, FunIf) of
      true -> eval(True, Point, As);
      false ->
         case False of
            undefined -> Point;
            _ -> eval(False, Point, As)
         end
   end.

eval(Fun, Point, As) when is_function(Fun) ->
   faxe_lambda:execute(Point, Fun, As);
eval(Val, Point, As) ->
   flowdata:set_field(Point, As, Val).


