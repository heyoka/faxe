%% Date: 17.01.20 - 18:38
%% Ⓒ 2020 heyoka
%% @doc
%% Evaluate a series of lambda expressions in a top down manner
%% the node will output / add the corresponding value of the first lambda expression that evaluates as true
%% @end
-module(esp_case).
-author("Alexander Minichmair").

%% API
-behavior(df_component).

-include("faxe.hrl").
%% API
-export([init/3, process/3, options/0, check_options/0]).


-record(state, {
   node_id,
   lambdas,
   values,
   as
}).

options() -> [
   {lambdas, lambda_list},
   {as, string},
   {values, string_list}
].

check_options() ->
   [
      {same_length, [lambdas, values]}
   ].

init(NodeId, _Ins, #{lambdas := LambdaFuns, as := As, values := Values}) ->
%%   lager:notice("~p init:node~n~p",[NodeId, Ps]),
   {ok, all, #state{lambdas = LambdaFuns, node_id = NodeId, as = As, values = Values}}.

process(_In, #data_batch{points = Points} = Batch,
      State = #state{lambdas = LambdaFuns, as = As, values = Values}) ->
   NewPoints = [eval(Point, LambdaFuns, Values, As) || Point <- Points],
   {emit, Batch#data_batch{points = NewPoints}, State};
process(_Inport, #data_point{} = Point, State = #state{lambdas = LambdaFuns, as = As, values = Values}) ->
   NewValue = eval(Point, LambdaFuns, Values, As),
%%   lager:info("~p emitting: ~p",[?MODULE, NewValue]),
   {emit, NewValue, State}.

eval(#data_point{} = P, [], [], As) ->
   flowdata:set_field(P, As, 0);
eval(#data_point{} = Point, [Lambda|Lambdas], [Value|Values], As) ->
   case faxe_lambda:execute(Point, Lambda) of
      true -> flowdata:set_field(Point, As, Value);
      false -> eval(Point, Lambdas, Values, As)
   end.