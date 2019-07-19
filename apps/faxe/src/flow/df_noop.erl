%% Date: 30.12.16 - 23:01
%% Ⓒ 2016 heyoka
-module(df_noop).
-author("Alexander Minichmair").

-include("dataflow.hrl").

-behavior(df_component).
%% API
-export([init/3, process/3, shutdown/1, options/0]).

options() ->
   [].

init(NodeId, _Inputs, _Args) ->
   ?LOG("~p init:node",[NodeId]),
   {ok, all, NodeId}.

process(_Inport, Value, State) ->
   ?LOG("~p process, ~p",[State, {_Inport, Value}]),

   {emit, Value, State}.

shutdown(_State) ->
   ?LOG("shutdown in ~p called",[?MODULE]).