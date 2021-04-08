%% Date: 15.02.2020 - 21:41
%% Ⓒ 2020 heyoka
%%
%% Union of multiple streams.
%% The union node takes the union of all of its parents as a simple pass through.
%% Data points received from each parent are passed onto child nodes without modification
%%
-module(esp_group_union).
-author("Alexander Minichmair").

-include("dataflow.hrl").

-behavior(df_component).
%% API
-export([init/3, process/3, options/0]).


options() -> [].

init(NodeId, _Inputs, #{}) ->
   lager:notice("PID group_union: ~p",[self()]),
   {ok, all, NodeId}.

process(_In, Item, NodeId) ->
   lager:warning("SubsScriptions : ~p",[df_subscription:list(NodeId)]),
   {emit, Item, NodeId}.