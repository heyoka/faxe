%% Date: 09.04.2021 - 21:41
%% Ⓒ 2020 heyoka
%%
%% Union of multiple grouped streams.
%%
-module(esp_group_union).
-author("Alexander Minichmair").

-behavior(df_component).
%% API
-export([init/3, process/3, options/0]).


options() -> [].

init(NodeId, _Inputs, #{}) ->
   {ok, all, NodeId}.

process(_In, Item, NodeId) ->
   {emit, Item, NodeId}.