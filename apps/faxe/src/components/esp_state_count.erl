%% Date: 15.07.2019 - 09:55
%% Ⓒ 2019 heyoka
%% @doc
%% Computes the number of consecutive points in a given state. The state is defined via a lambda expression.
%% For each consecutive point for which the expression evaluates as true,
%% the state count will be incremented.
%% When a point evaluates to false, the state count is reset.
%%
%% The state count will be added as an additional int64 field to each point.
%% If the expression evaluates to false, the value will be -1.
%% If the expression generates an error during evaluation, the point is discarded
%% and does not affect the state count.
%%
-module(esp_state_count).
-author("Alexander Minichmair").

%% API
-behavior(df_component).

-include("faxe.hrl").
%% API
-export([init/3, process/3, options/0]).

-record(state, {
   node_id,
   lambda,
   as,
   last_count = 0

}).

options() -> [{lambda, lambda}, {as, binary, <<"state_count">>}].

init(_NodeId, _Ins, #{lambda := Lambda, as := As}) ->
   {ok, all, #state{lambda = Lambda, as = As}}.

process(_In, #data_batch{points = _Points} = _Batch, _State = #state{lambda = _Lambda}) ->
   {error, not_implemented};
process(_Inport, #data_point{} = Point,
    State = #state{lambda = Lambda, last_count = LastTs, as = As}) ->
   case process_point(Point, Lambda, LastTs) of
      {ok, Count} ->
         NewPoint = flowdata:set_field(Point, As, Count),
         {emit, NewPoint, State#state{last_count = Count}};
      {error, _Error} ->
         {ok, State}
   end.


-spec process_point(#data_point{}, function(), non_neg_integer()) ->
   {ok, Count :: non_neg_integer()} | {error, term()}.
process_point(Point=#data_point{}, LFun, LastCount) ->
   case (catch exec(Point, LFun)) of
      true when LastCount > -1 -> {ok, LastCount+1};
      true -> {ok, 1};
      false -> {ok, -1};
      Error -> {error, Error}
   end.

exec(Point, LFun) -> faxe_lambda:execute(Point, LFun).



-ifdef(TEST).
process_point_test_() ->
   faxe_ets:start_link(),
   Point = #data_point{ts = 1234, fields = #{<<"value">> => 2134}},
   [point_count_test(Point), process_no_count_test(Point), process_first_count_test(Point)].
point_count_test(Point) ->
   LambdaString = "Value > 2133",
   ?_assertEqual(process_point(Point, lambda_helper(LambdaString), 17), {ok, 18}).
process_no_count_test(Point) ->
   LambdaString = "Value > 2134",
   ?_assertEqual(process_point(Point, lambda_helper(LambdaString), 17), {ok, -1}).
process_first_count_test(Point) ->
   LambdaString = "Value == 2134",
   ?_assertEqual(process_point(Point, lambda_helper(LambdaString), -1), {ok, 1}).
lambda_helper(LambdaString) ->
   faxe_dfs:make_lambda_fun(LambdaString, ["Value"], [<<"value">>]).
-endif.



