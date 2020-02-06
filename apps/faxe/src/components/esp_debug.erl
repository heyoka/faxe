%% Date: 30.12.16 - 23:01
%% Ⓒ 2019 heyoka
%%
%% the debug node just logs the incoming message with lager
%% and emits it without touching it in any way
%%
-module(esp_debug).
-author("Alexander Minichmair").

-include("dataflow.hrl").

-behavior(df_component).
%% API
-export([init/3, process/3, shutdown/1, options/0, check_options/0]).

-record(state, {
   level
}).

options() ->
   [{level, string, <<"notice">>}].

check_options() ->
   [
      {one_of, level,
         [<<"debug">>, <<"info">>, <<"notice">>, <<"warning">>, <<"error">>, <<"alert">>]}
   ].

init(_NodeId, _Inputs, #{level := Lev}) ->
   Level = binary_to_atom(Lev, latin1),
   {ok, all, #state{level = Level}}.

process(_Inport, Value, State=#state{level = Level}) ->
%%   lager:notice("process [at ~p] , ~p~n",[faxe_time:now(),  {_Inport, lager:pr(Value, ?MODULE)}]),
   Format = "process [at ~p] , ~p~n", Args = [faxe_time:now(),  {_Inport, lager:pr(Value, ?MODULE)}],
   lager:log(Level, self(), Format, Args),
   {emit, Value, State}.

shutdown(_State) ->
   ok.