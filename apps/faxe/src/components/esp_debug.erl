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
-export([init/3, process/3, shutdown/1, options/0, check_options/0, handle_info/2]).

-record(state, {
   nodeid,
   level,
   condition,
   msg,
   emit_debug = false
}).

options() ->
   [
      {level, string, <<"notice">>},
      {where, lambda, undefined},
      {message, string, <<"">>}
   ].

check_options() ->
   [
      {one_of, level,
         [<<"debug">>, <<"info">>, <<"notice">>,
            <<"warning">>, <<"error">>, <<"critical">>,<<"alert">>]}
   ].

init(NodeId, _Inputs, #{level := Lev, where := Cond, message := Msg}) ->
   Level = binary_to_atom(Lev, latin1),
   {ok, all, #state{level = Level, condition = Cond, msg = Msg, nodeid = NodeId}}.

process(Inport, Value, State=#state{}) ->
   %% immediately emit
   emit(Inport, Value, State),
   %% then maybe process
   maybe_process(Inport, Value, State),
   {ok, State}.

handle_info(start_debug, State) -> {ok, State#state{emit_debug = true}};
handle_info(stop_debug, State) -> {ok, State#state{emit_debug = false}};
handle_info(_, State) -> {ok, State}.

shutdown(_State) ->
   ok.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% internal %%%%%%%%%%%%%%%%%%%%%%%%%%%
maybe_process(Inport, Value, State=#state{condition = undefined}) ->
   do_process(Inport, Value, State);
maybe_process(Inport, Value, State=#state{condition = Fun}) ->
   case faxe_lambda:execute(Value, Fun) of
      true ->
         do_process(Inport, Value, State);
      false ->
         ok
   end.

do_process(Inport, Value, State=#state{level = Level}) ->
   {Format, Args} = build_msg(Inport, Value, State),
   do_log(Level, Format, Args).

build_msg(Inport, Item, #state{msg = Msg}) ->
   Format = "~s ~p~n",
   Args = [Msg,  {Inport, lager:pr(Item, ?MODULE)}],
   {Format, Args}.

emit(Port, Value, State = #state{nodeid = NodeIndex}) ->
   true = df_subscription:output(NodeIndex, Value, Port),
   node_metrics:metric(?METRIC_ITEMS_OUT, 1, NodeIndex),
   maybe_debug(item_out, Port, Value, State).

%% @doc emit debug events
maybe_debug(_Key, _Port, _Value, #state{emit_debug = false}) ->
   ok;
maybe_debug(Key, Port, Value, #state{emit_debug = true, nodeid = NodeIndex}) ->
   gen_event:notify(faxe_debug, {Key, NodeIndex, Port, Value}).

%% we need to do it this way, so we can keep our metadata in lager
do_log(debug, Format, Args) -> lager:debug(Format, Args);
do_log(info, Format, Args) -> lager:info(Format, Args);
do_log(notice, Format, Args) -> lager:notice(Format, Args);
do_log(warning, Format, Args) -> lager:warning(Format, Args);
do_log(error, Format, Args) -> lager:error(Format, Args);
do_log(critical, Format, Args) -> lager:critical(Format, Args);
do_log(alert, Format, Args) -> lager:critical(Format, Args).