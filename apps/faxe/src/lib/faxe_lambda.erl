%% Date: 06.04.17 - 22:48
%% Ⓒ 2017 heyoka
%%
%% @doc
%% A lambda in faxe is an erlang fun, which receives a #data_point record
%% and returns another fun, which is the actual lambda
%%
%% This module will evaluate both and return either the result of the inner-fun
%% or it will set #data_point's field or tag to the resulting value
%% @end
%%
-module(faxe_lambda).
-author("Alexander Minichmair").

-include("faxe.hrl").

%% API
-export([execute/3, execute/2, execute_bool/2]).

%% @doc
%% evaluates an erlang fun and returns the result
%% @end
-spec execute(#data_point{}, fun()) -> any().
execute(#data_point{} = Point, LambdaFun) when is_function(LambdaFun) ->
   PreparedFun = LambdaFun(Point),
   PreparedFun().

%% @doc
%% evaluates an erlang fun and returns the given data_point with the result of the evaluation
%% set to the field named As
%% @end
-spec execute(#data_point{}, fun(), binary()) -> any().
execute(#data_point{} = Point, LambdaFun, As) ->
   LVal = execute(Point, LambdaFun),
   flowdata:set_field(Point, As, LVal).

%% @doc
%% evaluate lambda and check if return type is boolean
%% does not cast to a boolean value !
%% @end
-spec execute_bool(#data_point{}, fun()) -> true | false | term().
execute_bool(Point, Lambda) ->
%%   case dfs_std_lib:bool(execute(Point, Lambda)) of
   case execute(Point, Lambda) of
      true -> true;
      false -> false;
      Else ->
         lager:warning("Lamdba ~p has no boolean return value on item: ~p (returned: ~p).",
         [faxe_util:stringize_lambda(Lambda), Point, Else]),
         Else
   end.
