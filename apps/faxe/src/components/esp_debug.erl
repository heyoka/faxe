%% Date: 30.12.16 - 23:01
%% Ⓒ 2019 heyoka
%%
%% the debug node just logs the incoming message with lager
%% and emits it without touching it in any way
%%
-module(esp_debug).
-author("Alexander Minichmair").

-include("faxe.hrl").

-behavior(df_component).
%% API
-export([init/3, process/3, shutdown/1, options/0, check_options/0]).

-record(state, {
   level
}).

options() ->
   [
      {level, string, <<"notice">>}
   ].

check_options() ->
   [
      {one_of, level,
         [<<"debug">>, <<"info">>, <<"notice">>,
            <<"warning">>, <<"error">>, <<"critical">>,<<"alert">>]}
   ].

init(_NodeId, _Inputs, #{level := Lev}) ->
   Level = binary_to_atom(Lev, latin1),
   {ok, all, #state{level = Level}}.

process(_Inport, Value, State=#state{level = Level}) ->
   Format = "[at ~p], ~p~n", Args = [faxe_time:now(),  {_Inport, lager:pr(Value, ?MODULE)}],
   do_log(Level, Format, Args),
   {emit_ack, Value, Value#data_point.dtag, State}.

shutdown(_State) ->
   ok.

%% we need to do it this way, so we can keep out metadata in lager
do_log(debug, Format, Args) -> lager:debug(Format, Args);
do_log(info, Format, Args) -> lager:info(Format, Args);
do_log(notice, Format, Args) -> lager:notice(Format, Args);
do_log(warning, Format, Args) -> lager:warning(Format, Args);
do_log(error, Format, Args) -> lager:error(Format, Args);
do_log(critical, Format, Args) -> lager:critical(Format, Args);
do_log(alert, Format, Args) -> lager:critical(Format, Args).