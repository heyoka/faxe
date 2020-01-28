%% Date: 27.01.20 - 18:38
%% Ⓒ 2020 heyoka
%% @doc
%% emits a message, if there is no message comming in for the given amount of time
%% @end
-module(esp_deadman).
-author("Alexander Minichmair").

%% API
-behavior(df_component).

-include("faxe.hrl").
%% API
-export([init/3, process/3, options/0, handle_info/2]).


-record(state, {
   node_id,
   timeout,
   message,
   as,
   timer_ref
}).

options() -> [
   {timeout, duration},
   {message, string},
   {as, string}
].

init(NodeId, _Ins, #{timeout := Timeout0, message := Message, as := As}) ->
   Timeout = faxe_time:duration_to_ms(Timeout0),
   State = #state{timeout = Timeout, message = Message, node_id = NodeId, as = As},
   {ok, all, restart_timer(State)}.

process(_In, _Data, State = #state{}) ->
   {ok, restart_timer(State)}.

handle_info(timeout, State = #state{message = Msg, as = As}) ->
   DataPoint = #data_point{ts = faxe_time:now()},
   dataflow:emit(flowdata:set_field(DataPoint, As, Msg)),
   {ok, restart_timer(State)}.

restart_timer(State = #state{timer_ref = TRef, timeout = Timeout}) ->
   catch erlang:cancel_timer(TRef),
   NewTimer = erlang:send_after(Timeout, self(), timeout),
   State#state{timer_ref = NewTimer}.
