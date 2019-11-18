%%%-------------------------------------------------------------------
%%% @author heyoka
%%% @copyright (C) 2019, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 18. Nov 2019 10:51
%%%-------------------------------------------------------------------
-module(lambda_tests).
-author("heyoka").

%% API
-include("faxe.hrl").
-compile(nowarn_export_all).
-compile([export_all]).
%%-ifdef(TEST).
%%-include_lib("eunit/include/eunit.hrl").

grip_rewrite() ->
   Point = flowdata:from_json_struct(grip_data(), <<"UTC-Time">>, <<"float_micro">>),
   ?assertEqual(#data_point{fields = #{<<"sku_height">> => 0.02,
      <<"sku_dim_confidence">> => 0.8,
      <<"target_lc_type">> => <<"Dark Gray Tote">>,
      <<"pick_quantity">> => 1.0,
      <<"sku_weight_confidence">> => 0.8,
      <<"pcs_multipicked">> => 0,
      <<"target_lc_id">> => <<>>,
      <<"source_lc_type">> => <<"Dark Gray Tote">>,
      <<"unsuccessful_trials">> => 0,
      <<"sku_weight">> => 0.159,
      <<"picked_too_few">> => 0,
      <<"sku_name">> => <<"red_tgw_tshirt">>,
      <<"source_location">> => <<"PP2">>,
      <<"target_location">> => <<"DP2">>,
      <<"sku_shape">> => 0,
      <<"Date-Time">> => <<"2019_11_18__09-51-59">>,
      <<"ol_error_pretty">> =>
      <<"finished successfully">>,
      <<"sku_drop_mode_pretty">> => <<"DROP">>,
      <<"source_lc_quantity">> => 10.0,
      <<"quantity_picked">> => 1.0,
      <<"source_lc_id">> => <<>>,
      <<"sku_width">> => 0.16,
      <<"robot_order_id">> => <<"1">>,
      <<"UTC-Time">> => <<"1574067119.037648">>,
      <<"sku_drop_mode">> => 0,
      <<"robot_order_timestamp">> =>
      <<"2019_11_18__09-51-51">>,
      <<"sku_length">> => 0.23,
      <<"total_duration_in_sec">> => 6.87994,
      <<"is_prepick">> => 0,
      <<"pcs_picked_board">> => 0,
      <<"pcs_lost">> => 0.0,
      <<"avg_duration_per_pick">> => 6.87994,
      <<"overheight_correction_trials">> => 0,
      <<"ol_error">> => <<"{}">>,
      <<"sku_shape_pretty">> => <<"DUCTILE">>,
      <<"grasped_too_many">> => 0}, ts = 1574067119038},
      Point),

      VarList = ["sku_length",
         "sku_width",
         "sk_height",
         "sku_weight",
         "source_lc_quantity",
         "pcs_lost",
         "pick_quantity",
         "quantity_picked"],

      LambdaList = [
         "dfs_std_lib:int(Sku_length * 1000)",
         "dfs_std_lib:int(Sku_width * 1000)",
         "dfs_std_lib:int(Sk_height * 1000)",
         "dfs_std_lib:int(Sku_weight * 1000)",
         "dfs_std_lib:int(Source_lc_quantity)",
         "dfs_std_lib:int(Pcs_lost)",
         "dfs_std_lib:int(Pick_quantity)",
         "dfs_std_lib:int(Quantity_picked)"
      ],
   F =
      fun({Lambda, Var}, P=#data_point{}) ->
         faxe_lambda:execute(P, lambda_helper(Lambda, [Var]), list_to_binary(Var))
      end,
   NewPoint = lists:foldl(F, Point, lists:zip(LambdaList, VarList)),
   ?assertEqual(#data_point{fields = #{<<"sku_height">> => 0.02,
      <<"sku_dim_confidence">> => 0.8,
      <<"target_lc_type">> => <<"Dark Gray Tote">>,
      <<"pick_quantity">> => 1,
      <<"sku_weight_confidence">> => 0.8,
      <<"pcs_multipicked">> => 0,
      <<"target_lc_id">> => <<>>,
      <<"source_lc_type">> => <<"Dark Gray Tote">>,
      <<"unsuccessful_trials">> => 0,
      <<"sku_weight">> => 159,
      <<"picked_too_few">> => 0,
      <<"sku_name">> => <<"red_tgw_tshirt">>,
      <<"source_location">> => <<"PP2">>,
      <<"target_location">> => <<"DP2">>,
      <<"sku_shape">> => 0,
      <<"Date-Time">> => <<"2019_11_18__09-51-59">>,
      <<"ol_error_pretty">> =>
      <<"finished successfully">>,
      <<"sku_drop_mode_pretty">> => <<"DROP">>,
      <<"source_lc_quantity">> => 10,
      <<"quantity_picked">> => 1,
      <<"source_lc_id">> => <<>>,
      <<"sku_width">> => 160,
      <<"robot_order_id">> => <<"1">>,
      <<"UTC-Time">> => <<"1574067119.037648">>,
      <<"sku_drop_mode">> => 0,
      <<"robot_order_timestamp">> =>
      <<"2019_11_18__09-51-51">>,
      <<"sku_length">> => 230,
      <<"total_duration_in_sec">> => 6.87994,
      <<"is_prepick">> => 0,
      <<"pcs_picked_board">> => 0,
      <<"pcs_lost">> => 0.0,
      <<"avg_duration_per_pick">> => 6.87994,
      <<"overheight_correction_trials">> => 0,
      <<"ol_error">> => <<"{}">>,
      <<"sku_shape_pretty">> => <<"DUCTILE">>,
      <<"grasped_too_many">> => 0}, ts = 1574067119038},
      NewPoint).

lambda_helper(LambdaString, VarList) ->
   VarBindings = [{string:titlecase(Var), list_to_binary(string:lowercase(Var))} || Var <- VarList],
   {Vars, Bindings} = lists:unzip(VarBindings),
   faxe_dfs:make_lambda_fun(LambdaString, Vars, Bindings).

lambda_helper_test() ->
   VarList = ["var1", "someothervar2"],
   VarBindings = [{string:titlecase(Var), list_to_binary(string:lowercase(Var))} || Var <- VarList],
   {Vars, Bindings} = lists:unzip(VarBindings),
   ?assertEqual(Vars, ["Var1", "Someothervar2"]),
   ?assertEqual(Bindings, [<<"var1">>, <<"someothervar2">>]).


grip_data() ->
 <<"{\"UTC-Time\":\"1574067119.037648\",
    \"Date-Time\":\"2019_11_18__09-51-59\",\"robot_order_id\":\"1\",
 \"robot_order_timestamp\":\"2019_11_18__09-51-51\",\"is_prepick\":0,\"source_location\":\"PP2\",
 \"target_location\":\"DP2\",\"sku_shape\":0,\"sku_shape_pretty\":\"DUCTILE\",\"sku_drop_mode\":0,
 \"sku_drop_mode_pretty\":\"DROP\",\"sku_name\":\"red_tgw_tshirt\",\"sku_length\":0.230,\"sku_width\":0.160,
 \"sku_height\":0.020,\"sku_dim_confidence\":0.80,\"sku_weight\":0.1590,\"sku_weight_confidence\":0.80,
 \"source_lc_id\":\"\",\"source_lc_type\":\"Dark Gray Tote\",\"source_lc_quantity\":10.0,\"target_lc_id\":\"\",
 \"target_lc_type\":\"Dark Gray Tote\",\"ol_error\":\"{}\",\"ol_error_pretty\":\"finished successfully\",
 \"pcs_multipicked\":0,\"pcs_picked_board\":0,\"pcs_lost\":0.0,\"unsuccessful_trials\":0,\"grasped_too_many\":0,
 \"overheight_correction_trials\":0,\"picked_too_few\":0,\"pick_quantity\":1.0,\"quantity_picked\":1.0,
 \"total_duration_in_sec\":6.87994,\"avg_duration_per_pick\":6.87994}">>.


%%-endif.
