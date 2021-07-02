%% Date: 30.12.16 - 23:01
%% Ⓒ 2019 heyoka
%%
%% the jsonsize node converts incoming values to json and logs the resulting byte-size
%% emits data without touching it in any way
%%
-module(esp_jsonsize).
-author("Alexander Minichmair").

-include("faxe.hrl").

-behavior(df_component).
%% API
-export([init/3, process/3, shutdown/1, options/0]).

-record(state, {
   inject,
   fieldname
}).


options() ->
   [{inject, is_set}, {field, string, <<"jsonsize">>}].

init(_NodeId, _Inputs, #{inject := Inject, field := FName}) ->
   {ok, all, #state{inject = Inject, fieldname = FName}}.

process(_Inport, Value, State = #state{inject = Inject, fieldname = FName}) ->
   Json = flowdata:to_json(Value),
   NewValue =
   case Inject of
      true -> flowdata:set_field(Value, FName, byte_size(Json));
      false -> lager:notice("[~p] json binary message size: ~p",[?MODULE, byte_size(Json)]), Value
   end,
   {emit, NewValue, State}.

shutdown(_State) ->
   ok.