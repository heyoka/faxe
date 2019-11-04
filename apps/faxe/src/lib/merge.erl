%%%-------------------------------------------------------------------
%%% @author heyoka
%%% @copyright (C) 2019, <COMPANY>
%%% @doc
%%% experimental merging of 2 nested maps, also merges lists of maps contained in maps
%%% note: top level structures must be maps
%%% @end
%%% Created : 25. Oct 2019 12:23
%%%-------------------------------------------------------------------
-module(merge).
-author("heyoka").

%% API
-export([merge/3, test/0, merge/2]).

-define(MFUN, fun
                 F(_K, {left, V}) -> {ok, V};
                 F(_K, {right, V}) -> {ok, V};
                 F(_K, {both, L, R}) when is_map(L), is_map(R) -> {ok, merge(F, L, R)};
                 F(_K, {both, [L], [R]}) when is_map(L), is_map(R) -> {ok, [merge(F, L, R)]}; %% arbitrary choice
                 F(_K, {both, L, R}) when is_list(L), is_list(R) -> {ok, L++R}; %% arbitrary choice
                 F(_K, {both, L, R}) -> {ok, [L, R]} %% arbitrary choice
              end).

merge(Left, Right) ->
   merge(?MFUN, Left, Right).

merge(F, L, R) ->
   L1 = lists:sort(maps:to_list(L)),
   L2 = lists:sort(maps:to_list(R)),
   merge(F, L1, L2, []).

merge(_F, [], [], Acc) -> maps:from_list(Acc);
merge(F, [{KX, VX}|Xs], [], Acc) ->
   merge(F, Xs, [], f(KX, F(KX, {left, VX}), Acc));
merge(F, [], [{KY, VY} | Ys], Acc) ->
   merge(F, [], Ys, f(KY, F(KY, {right, VY}), Acc));
merge(F, [{KX, VX}|Xs] = Left, [{KY,VY}|Ys] = Right, Acc) ->
   if
      KX < KY -> merge(F, Xs, Right, f(KX, F(KX, {left, VX}), Acc));
      KX > KY -> merge(F, Left, Ys, f(KY, F(KY, {right, VY}), Acc));
      KX =:= KY -> merge(F, Xs, Ys, f(KX, F(KX, {both, VX, VY}), Acc))
   end.

f(_K, undefined, Acc) -> Acc;
f(K, {ok, R}, Acc) -> [{K, R} | Acc].


test() ->
   M1 =
      #{<<"data">> =>
         #{<<"tbo">> =>
            [
               #{<<"Dir1_Out_HO">> => 0,<<"Dir1_Out_TO">> => 0,<<"QX_Err">> => 0,
                  <<"QX_StatCnvDrvBwd">> => 0,<<"QX_StatCnvDrvFwd">> => 0,<<"QX_StatLiftDown">> => 0,
                  <<"QX_StatLiftDrvUp">> => 0,<<"ix_CcRdy">> => 1,<<"ix_Lift_PosBo">> => 1,
                  <<"ix_Lift_PosTop">> => 0,<<"ix_OcM1">> => 0,<<"ix_OcM2">> => 1,
                  <<"moduleNo">> => 1140}]}},
   M2 =
      #{<<"data">> =>
         #{<<"tbo">> =>
         [
            #{<<"Dir1_Out_DrvRun">> => 0,<<"Dir1_Out_PosBot">> => 0,<<"Dir1_Out_PosMid">> => 0,
               <<"Dir3_Out_DrvRun">> => 0,<<"Dir3_Out_HO">> => 0,<<"Dir3_Out_PosBot">> => 1,
               <<"Dir3_Out_PosMid">> => 0,<<"Dir3_Out_PosTop">> => 0,<<"Dir3_Out_TO">> => 0,
               <<"moduleNo">> => 1140}]}},
   merge(?MFUN, M1, M2).
