%% Date: 15.04.17 - 10:52
%% Ⓒ 2017 LineMetrics GmbH
%% mappings for dfs params on node statements
%%
-module(faxe_node_params).
-author("Alexander Minichmair").

%% API
-compile(export_all).

%% [{param_number, port_number}]
params(<<"combine">>) -> [{1, 2}];
params(<<"join">>) -> [{all, 2}].

options(<<"deadman">>) -> [{1, <<"interval">>, duration},{2, <<"threshold">>, int}];
options(<<"shift">>) -> [{1, <<"offset">>, duration}];
options(<<"eval">>) -> [{all, <<"lambdas">>, lambda_list}];
options(_) -> undefined.