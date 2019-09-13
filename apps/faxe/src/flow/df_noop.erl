%% Date: 30.12.16 - 23:01
%% Ⓒ 2016 heyoka
%% @doc node that does: nothing
-module(df_noop).
-author("Alexander Minichmair").

-include("dataflow.hrl").

-behavior(df_component).
%% API
-export([init/3, process/3, options/0]).

options() ->
   [].

init(NodeId, _Inputs, _Args) ->
   {ok, all, NodeId}.

process(_Inport, Value, State) ->
   {emit, Value, State}.