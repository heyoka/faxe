%% Date: 30.12.16 - 23:01
%% Ⓒ 2019 heyoka
-module(esp_log).
-author("Alexander Minichmair").

-include("dataflow.hrl").

-behavior(df_component).
%% API
-export([init/3, process/3, shutdown/1, options/0]).

-record(state, {
   file :: list()
}).

options() ->
   [{file, string}].

init(NodeId, _Inputs, #{file := File}) ->
   lager:info("~p init:node",[NodeId]),
   {ok, all, NodeId}.

process(_Inport, P = #data_point{fields = Fields}, State = #state{file = FName}) ->
   F = file:open(FName, [write]),
   io:format(F, "~s~n", [Fields]),
   {ok, State};
process(_In, B = #data_batch{points = Ps}, State) ->
   [process(_In, P, State) || P <- Ps],
   {ok, State}.

shutdown(_State) ->
   lager:info("shutdown in ~p called",[?MODULE]).