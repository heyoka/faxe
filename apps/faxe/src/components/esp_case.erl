%% Date: 17.01.20 - 18:38
%% Ⓒ 2020 heyoka
%% @doc
%% Evaluate a series of lambda expressions in a top down manner
%% the node will output / add the corresponding value of the first lambda expression that evaluates as true
%% if none of the lambda expressions evaluates as true, a default value will be used
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
   as,
   default
}).

options() -> [
   {lambdas, lambda_list},
   {as, string},
   {values, string_list},
   {default, any}
].

check_options() ->
   [
      {same_length, [lambdas, values]}
   ].

init(NodeId, _Ins, #{lambdas := LambdaFuns, as := As, values := Values, default := Default}) ->
   {ok, all, #state{lambdas = LambdaFuns, node_id = NodeId, as = As, values = Values, default = Default}}.

process(_In, #data_batch{points = Points} = Batch,
      State = #state{lambdas = LambdaFuns, as = As, values = Values, default = Def}) ->
   NewPoints = [eval(Point, LambdaFuns, Values, As, Def) || Point <- Points],
   {emit, Batch#data_batch{points = NewPoints}, State};
process(_Inport, #data_point{} = Point,
    State = #state{lambdas = LambdaFuns, as = As, values = Values, default = Def}) ->
   NewValue = eval(Point, LambdaFuns, Values, As, Def),
   {emit, NewValue, State}.

eval(#data_point{} = P, [], [], As, Default) ->
   flowdata:set_field(P, As, Default);
eval(#data_point{} = Point, [Lambda|Lambdas], [Value|Values], As, Default) ->
   case faxe_lambda:execute(Point, Lambda) of
      true -> flowdata:set_field(Point, As, Value);
      false -> eval(Point, Lambdas, Values, As, Default)
   end.