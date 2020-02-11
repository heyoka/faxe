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
   default,
   json
}).

options() -> [
   {lambdas, lambda_list},
   {as, string}, %% key
   {values, list}, %% list of values
   {json, is_set, false}, %% treat the values as json strings
   {default, any}
].

check_options() ->
   [
      {same_length, [lambdas, values]}
   ].

init(NodeId, _Ins,
    #{lambdas := LambdaFuns, as := As, values := Values0, default := Default0, json := Json}) ->

   Default = case Json of true -> jiffy:decode(Default0, [return_maps]); false -> Default0 end,
   Values =
   case Json of
      true -> [jiffy:decode(Val, [return_maps]) || Val <- Values0];
      false -> Values0
   end,
   {ok, all,
      #state{lambdas = LambdaFuns, node_id = NodeId,
         as = As, values = Values, default = Default, json = Json}}.

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