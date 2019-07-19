%% Date: 30.12.16 - 23:01
%% Ⓒ 2019 heyoka
-module(esp_debug).
-author("Alexander Minichmair").

-include("dataflow.hrl").

-behavior(df_component).
%% API
-export([init/3, process/3, shutdown/1, options/0]).

options() ->
   [].

init(NodeId, _Inputs, _Args) ->
   lager:info("~p init:node",[NodeId]),
   {ok, all, NodeId}.

process(_Inport, Value, State) ->
   lager:notice("~p process, ~p",[State, {_Inport, Value}]),

   {ok, State}.

shutdown(_State) ->
   lager:info("shutdown in ~p called",[?MODULE]).