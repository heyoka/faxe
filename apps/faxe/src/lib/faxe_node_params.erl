%% Date: 15.04.17 - 10:52
%% Ⓒ 2017 heyoka
%% mappings for dfs params on node statements
%%
-module(faxe_node_params).
-author("Alexander Minichmair").

%% API
-compile(nowarn_export_all).
-compile(export_all).

%% [{param_number, [mode,] port_number}] | {param_number, mode, port_number}
params(<<"combine">>) -> [{1, 2}];
params(<<"merge">>) -> [{1, 2}];
params(<<"join">>) -> {all, new_port, 1};
params(<<"union">>) -> {all, new_port, 1};
params(<<"group_union">>) -> {all, new_port, 1};
params(<<"state_sequence">>) -> {all, new_port, 1};
params(<<"triggered_timeout">>) -> {all, new_port, 1};
params(_) -> undefined.

%% (component name binary) -> [{param number|all, param name, param_type}]
options(<<"deadman">>) -> [{1, <<"timeout">>, duration},{2, <<"threshold">>, integer}];
options(<<"shift">>) -> [{1, <<"offset">>, duration}];
options(<<"batch">>) -> [{1, <<"size">>, integer}];
options(<<"where">>) -> [{1, <<"lambda">>, lambda}];
options(<<"eval">>) -> [{all, <<"lambdas">>, lambda_list}];
options(<<"keep">>) -> [{all, <<"fields">>, binary_list}];
options(<<"delete">>) -> [{all, <<"fields">>, binary_list}];
options(<<"log">>) -> [{1, <<"file">>, string}];
options(<<"sample">>) -> [{1, <<"rate">>, string}];
options(<<"state_count">>) -> [{1, <<"lambda">>, lambda}];
options(<<"state_duration">>) -> [{1, <<"lambda">>, lambda}];
options(<<"state_change">>) -> [{1, <<"lambda">>, lambda}];
options(<<"debug">>) -> [{1, <<"level">>, string}];
options(<<"case">>) -> [{all, <<"lambdas">>, lambda_list}];
options(<<"change_detect">>) -> [{all, <<"fields">>, string_list}];
options(<<"collect_unique">>) -> [{1, <<"field">>, string}];
options(<<"group_by">>) -> [{all, <<"fields">>, string_list}];
options(_) -> undefined.

%% helper functions
