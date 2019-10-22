%% Date: 30.12.16 - 23:01
%% Log everything that comes in to a file, line by line
%% Ⓒ 2019 heyoka
%%
-module(esp_log).
-author("Alexander Minichmair").

-include("dataflow.hrl").

-behavior(df_component).
%% API
-export([init/3, process/3, options/0, check_options/0]).

-record(state, {
   file :: list()
}).

options() ->
   [{file, string}].

check_options() ->
   [{not_empty, [file]}].

init(_NodeId, _Inputs, #{file := File}) ->
   {ok, F} = file:open(File, [write]),
   {ok, all, #state{file = F}}.

process(_In, P = #data_point{}, State = #state{file = F}) ->
   do_log(P, F),
   {emit, P, State};
process(_In, B = #data_batch{points = Ps}, State = #state{file = F}) ->
   [do_log(P, F) || P <- Ps],
   {emit, B, State}.

do_log(P, File) when is_list(P) ->
   lager:warning("Point is a LIST!: ~p",[P]);
do_log(P, File) ->
   io:format(File, "~s~n", [binary_to_list(flowdata:to_json(P))]).