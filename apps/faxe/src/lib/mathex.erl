%%%-------------------------------------------------------------------
%%% @author nisbus <nisbus@gmail.com>
%%% @author Alexander Minichmair
%%% @copyright (C) 2013, 2016
%%% @doc
%%%   A statistics module with some common statistics.
%%%   Take note that this lib will not handle non numerical data
%%%   gracefully.
%%% @end
%%% Created : 23 Feb 2013 by nisbus <nisbus@gmail.com>
%%% Extended : 10.01.2016 by Alexander Minichmair
%%%-------------------------------------------------------------------
-module(mathex).

%% API
-export([
   moving_average/1, average/1, sum/1,
   stdev_sample/1, stdev_population/1,
   skew/1, kurtosis/1, variance/1,
   covariance/2, correlation/2, correlation_matrix/1,
   nth_root/2, percentile/2, zscore/1]).
%%%===================================================================
%%% API
%%%===================================================================
-spec moving_average(Data :: [] | [float()|integer]) -> []|[float()].
moving_average([]) ->
   [];
moving_average([H|_] = Data) when is_list(Data) ->
   {_,Res,_} = lists:foldl(fun(X,Acc) ->
      {Count, Res,CurrentAv} = Acc,
      case Count of
         0 -> {1, [X], X};
         _ ->
            Cav = ((CurrentAv * Count+X))/(Count+1),
            {Count+1,[Cav|Res], Cav}
      end
                           end,{0,[],H},Data),
   lists:reverse(Res).

-spec stdev_sample(Data :: [] | [float()|integer()]) -> float().
stdev_sample([])->
   0.0;
stdev_sample(Data) when is_list(Data) ->
   C = length(Data),
   A = average(Data),
   math:sqrt(lists:foldl(fun(X,Acc) ->
      Acc+math:pow(X-A,2)/ (C-1)
                         end, 0,Data)).

-spec stdev_population(Data :: [] | [float()|integer()]) -> float().
stdev_population([]) ->
   0.0;
stdev_population(Data) when is_list(Data) ->
   A = average(Data),
   math:sqrt(average(lists:map(fun(X) ->
      math:pow(X-A,2)
                               end,Data))).

-spec skew(Data :: [] | [float()|integer()]) -> float().
skew([])->
   0.0;
skew(Data) when is_list(Data) ->
   A = average(Data),
   C = length(Data),
   Std = stdev_sample(Data),
   Mult = (C)/((C-1)*(C-2)),
   sum(lists:map(fun(X) ->
      math:pow(((X-A)/Std),3)
                 end,Data))*Mult.

-spec kurtosis(Data :: [] | [float()|integer()]) -> float().
kurtosis([]) ->
   0.0;
kurtosis(Data) when is_list(Data) ->
   A = average(Data),
   C = length(Data),
   Std = stdev_sample(Data),
   case Std =:= 0.0 of
      true -> 0.0;
      false ->
         Mult = ((C) * (C+1)) / ((C-1) * (C -2) * (C - 3)),
         Sub = 3 * (math:pow(C-1,2)) / ((C-2) * (C -3)),
         Mult * sum(lists:map(fun(X) ->
            math:pow( ((X-A)/Std),4)
                              end,Data))-Sub
   end
   .

-spec variance(Data :: [] | [float()|integer()]) -> float().
variance([]) ->
   0.0;
variance(Data) when is_list(Data) ->
   A = average(Data),
   C = length(Data),
   sum(lists:map(fun(X) ->
      math:pow(X - A,2)
                 end,Data))/C.

-spec covariance(Xs :: [] | [float()|integer()],Ys :: [] | [float()|integer()]) -> float().
covariance([],_) ->
   0.0;
covariance(_,[]) ->
   0.0;
covariance(Xs,Ys) when is_list(Xs) and is_list(Ys) ->
   AveX = average(Xs),
   AveY = average(Ys),
   sum(lists:zipwith(fun(X,Y)->
      (X-AveX)*(Y-AveY)
                     end,Xs,Ys))/(length(Xs)-1).

-spec correlation(Xs :: [] | [float()|integer()],Ys :: [] | [float()|integer()]) -> float().
correlation([],_) ->
   0.0;
correlation(_,[]) ->
   0.0;
correlation(Xs,Ys) ->
   covariance(Xs,Ys) /(stdev_sample(Xs)*stdev_sample(Ys)).

-spec correlation_matrix(ListOfLists :: [] | [[float()|integer()]]) -> [{integer(),integer(),float()}].
correlation_matrix([]) ->
   [];
correlation_matrix(ListOfLists) ->
   {_,R} = lists:foldl(fun(X,{Index,Res}) ->
      {_,C} = get_correlations(Index,X,ListOfLists),
      {Index+1,[{Index,C}|Res]}
                       end, {1,[]},ListOfLists),
   lists:reverse(lists:flatten(lists:map(fun({L,Res}) ->
      lists:map(fun({I,X}) ->
         {L,I,X}
                end,Res)
                                         end,R))).

-spec average(Data :: [] | [float()|integer()]) -> float().
average([]) ->
   0.0;
average(Data) ->
   sum(Data)/length(Data).

-spec sum(Data :: [] | [float()|integer()]) -> float()|integer().
sum([]) ->
   0.0;
sum(Data) ->
   lists:foldl(fun(X,Acc) ->
      Acc+X
               end,0,Data).

%% compute nth-root of X
nth_root(N, X) -> nth_root(N, X, 1.0e-5).
nth_root(N, X, Precision) ->
   F = fun(Prev) -> ((N - 1) * Prev + X / math:pow(Prev, (N-1))) / N end,
   fixed_point(F, X, Precision).



-spec percentile(list(), number()) -> float().
percentile([], _) -> 0;
percentile(List, 0) -> lists:min(List);
percentile(List, 100) -> lists:max(List);
percentile(List, N) when is_list(List) andalso is_number(N) ->
   S = lists:sort(List),
   R = N/100.0 * length(List),
   F = trunc(R),
   Lower = lists:nth(F, S),
   Upper = lists:nth(F + 1, S),
   Lower + (Upper - Lower) * (R - F)
.


-spec zscore(list()) -> list().
zscore(List) when is_list(List) ->
   Lmean = average(List),
   Lstdev = stdev_sample(List),
   lists:map(fun(N) -> (N - Lmean)/Lstdev  end, List).

%%%===================================================================
%%% Internal functions
%%%===================================================================
get_correlations(Index,Item,ListOfLists) ->
   lists:foldl(fun(X,{Idx,Res}) ->
      case Idx =:= Index of
         true -> {Idx+1,Res};
         false ->
            Correl = correlation(Item,X),
            {Idx+1,[{Idx,Correl}|Res]}
      end
               end,{1,[]},ListOfLists).

%% for nth-root func
fixed_point(F, Guess, Tolerance) ->
   fixed_point(F, Guess, Tolerance, F(Guess)).
fixed_point(_, Guess, Tolerance, Next) when abs(Guess - Next) < Tolerance ->
   Next;
fixed_point(F, _, Tolerance, Next) ->
   fixed_point(F, Next, Tolerance, F(Next)).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%%
%%-define(log2denom, 0.69314718055994529).
%%
%%transform(#value_transform{} = T, #data_item{value = Val, min = Min, max = Max, avg = Avg} = Item) ->
%%   TransFun = transform_fun(T),
%%   New = Item#data_item{
%%      value = trans(TransFun, Val),
%%      min   = trans(TransFun, Min),
%%      max   = trans(TransFun, Max),
%%      avg   = trans(TransFun, Avg)
%%   },
%%   lager:debug("transform ITEM: ~p TO : ~p~n",[Item, New]),
%%   New.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%%%%%%%%%%% internal %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%
%%%% get the transform higher-order function
%%transform_fun(#value_transform{mode = <<"lin">>, range = [LowMark, LowVal, HiMark, HiVal]}) ->
%%   Factor =  (HiVal-LowVal) / (HiMark-LowMark),
%%   fun(V) -> LowVal + Factor * V end
%%;
%%transform_fun(#value_transform{mode = <<"ln">>}) ->
%%   fun(V) -> math:log(V) end
%%;
%%transform_fun(#value_transform{mode = <<"log2">>}) ->
%%   fun(V) -> log2(V) end
%%;
%%transform_fun(#value_transform{mode = <<"log10">>}) ->
%%   fun(V) -> math:log10(V) end
%%.
%%
%%%% do it
%%trans(Fun, Val) when is_integer(Val) orelse is_float(Val) ->
%%   Fun(Val)
%%;
%%trans(_Fun, Val) ->
%%   Val
%%.
%%
%%
%%%%natural log (base e)
%%%% math:log(X) === ln
%%
%%%% log for base 2
%%log2(X) ->
%%   math:log(X) / ?log2denom.
%%
%%%% log for base N
%%logN(N, X) ->
%%   math:log(X) / math:log(N).