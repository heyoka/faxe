%% Date: 30.12.16 - 23:01
%% Ⓒ 2019 heyoka
%%
%% the debug node just logs the incoming message with lager
%% and emits it without touching it in any way
%%
-module(esp_jsonsize).
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
   Json = flowdata:to_json(Value),
   lager:notice("[~p] json binary message size: ~p",[?MODULE, byte_size(Json)]),
   {emit, Value, State}.

shutdown(_State) ->
   ok.