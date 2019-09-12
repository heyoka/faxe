%% Date: 30.12.16 - 23:01
%% Ⓒ 2019 heyoka
%%
%% the debug node just logs the incomming message with lager
%% and emits it without touching it in any way
%%
-module(esp_debug).
-author("Alexander Minichmair").

-include("dataflow.hrl").

-behavior(df_component).
%% API
-export([init/3, process/3, shutdown/1, options/0]).

options() ->
   [].

init(NodeId, _Inputs, _Args) ->
   {ok, all, NodeId}.

process(_Inport, Value, State) ->
   lager:notice("~p process [at ~p] , ~p",[State, faxe_time:now(),  {_Inport, Value}]),
   {emit, Value, State}.

shutdown(_State) ->
   ok.