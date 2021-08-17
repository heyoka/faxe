%%%-------------------------------------------------------------------
%%% @author heyoka
%%% @copyright (C) 2021
%%% @doc
%%%
%%% @end
%%% Created : 01. Aug 2021 21:08
%%%-------------------------------------------------------------------
-module(perf).
-author("heyoka").

-include("faxe.hrl").
%% API
-export([lambda_perf/0, lambda_perf/1, lambda_perf/2, execute/3]).

lambda_perf() ->
  lambda_perf(1, 10).
lambda_perf(Num) ->
  lambda_perf(Num, auto).
lambda_perf(Num, auto) ->
  Par = round((Num * 8) / 100),
  lambda_perf(Num, Par);
lambda_perf(Num, Par) ->
  Vars = ["dat.val2", "val1"],
  Refs = [<<"Dat_val2">>, <<"Val1">>],
  Lambda = " dfs_std_lib:int(Dat_val2 > 3) ",
%%  Lambda = " dfs_std_lib:int(Dat_val2 / 1000) + math:sqrt(Val1) / 4000",
  LambdaCall = lambda_helper(Lambda, Vars),
  Batch = make_batch(Num),
%%  lager:info("LambdaCall: ~p",[LambdaCall]),
  {TMy, _Res} = timer:tc(perf, execute, [Batch, LambdaCall, Par]),
  {time_to_process_lambdas, {items, Num}, {concurrency, Par}, {time_spent, erlang:round(TMy/1000), millis}}.

execute(#data_batch{points = Points}, LambdaCall, Par) ->
  plists:map(
   fun(Point) ->
    faxe_lambda:execute(Point, LambdaCall, <<"_">>)
   end,
    Points,
    Par
).

make_batch(Num) ->
  Points = [
    #data_point{
      ts=faxe_time:now(),
      fields=#{<<"val1">> => Z, <<"dat">> => #{<<"val2">> => Z*2}}}
    || Z <- lists:seq(1,Num)
  ],
  #data_batch{points = Points}.

lambda_helper(LambdaString, VarList) ->
  VarBindings = [{replace_dots(string:titlecase(Var)), list_to_binary(string:lowercase(Var))} || Var <- VarList],
%%  lager:notice("VarBindings: ~p",[VarBindings]),
  {Vars, Bindings} = lists:unzip(VarBindings),
  faxe_dfs:make_lambda_fun(LambdaString, Vars, Bindings).

replace_dots(String) ->
  binary_to_list(estr:str_replace(list_to_binary(String), <<".">>, <<"_">>)).