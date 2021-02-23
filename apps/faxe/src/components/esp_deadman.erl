%% Date: 27.01.20 - 18:38
%% Ⓒ 2020 heyoka
%% @doc
%% emits a point, if there is no message coming in for the given amount of time
%% for the output datapoint there are two options:
%% # if repeat_last param if set, the node will output the last message it saw incoming as the dead-message,
%%    if there is no last message yet, an empty message will be emitted
%% # multiple field and field_value can be provided to be included in the output
%%    if no fields (and field_values) parameter and is given, an empty datapoint will be emitted
%% the repeat_last parameter will always override the fields and field_values parameter
%%
%% the node will forward every message it gets by default, this can be changed by using the no_forward flag
%% @end
-module(esp_deadman).
-author("Alexander Minichmair").

%% API
-behavior(df_component).

-include("faxe.hrl").
%% API
-export([init/3, process/3, options/0, handle_info/2, check_options/0]).


-record(state, {
   node_id,
   timeout,
   fields,
   field_vals,
   timer_ref,
   silent_time,
   silent_timer_ref,
   is_quiet = false,
   trigger_on_value = false,
   repeat_last = false,
   repeat_with_new_ts = true,
   last_point,
   no_forward
}).

options() -> [
   {timeout, duration}, %%
   {fields, string_list, []},
   {field_values, list, []},
   {silent_time, duration, <<"0ms">>}, %% for this amount of time no timeout is triggered
   {repeat_last, is_set},
   {repeat_with_new_ts, bool, true},
   {trigger_on_value, is_set},
   {no_forward, is_set}
].

check_options() -> [{same_length, [fields, field_values]}].

init(NodeId, _Ins,
    #{timeout := Timeout0, fields := Fields, repeat_last := Repeat, no_forward := NoForward,
       trigger_on_value := Trigger, field_values := Vals, silent_time := QTime0, repeat_with_new_ts := NewTs}) ->


   Timeout = faxe_time:duration_to_ms(Timeout0),
   QTimeout = faxe_time:duration_to_ms(QTime0),
   State =
      #state{timeout = Timeout, fields = Fields, repeat_last = Repeat, no_forward = NoForward,
         repeat_with_new_ts = NewTs, node_id = NodeId, field_vals = Vals, silent_time = QTimeout,
         trigger_on_value = Trigger},

   {ok, all, maybe_trigger_restart_timer(State)}.

process(_In, Data, State = #state{no_forward = true}) ->
   NewState = State#state{last_point = Data},
   {ok, maybe_restart_timer(NewState)};
process(_In, Data, State = #state{}) ->
   NewState = State#state{last_point = Data},
   {emit, Data, maybe_restart_timer(NewState)}.

handle_info(q_timeout, State = #state{}) ->
%%   lager:info("silent timeout is up"),
   {ok, restart_timer(State#state{is_quiet = false, silent_timer_ref = undefined})};

handle_info(timeout, State = #state{}) ->
%%   lager:info("time is up"),
   dataflow:emit(build_message(State)),
   NewState = maybe_start_qtimer(State),
   {ok, maybe_restart_timer(NewState)};
handle_info(_R, State) ->
   {ok, State}.


build_message(#state{repeat_last = true, repeat_with_new_ts = NewTs, last_point = P=#data_point{}}) ->
   case NewTs of
      true -> P#data_point{ts = faxe_time:now()};
      false -> P
   end;
build_message(#state{field_vals = Vals, fields = Fields}) ->
   DataPoint = #data_point{ts = faxe_time:now()},
   flowdata:set_fields(DataPoint, Fields, Vals).

maybe_start_qtimer(State = #state{silent_time = 0}) ->
   State;
maybe_start_qtimer(State = #state{silent_time = QTime}) ->
%%   lager:notice("start silent_timer"),
   NewQTimer = erlang:send_after(QTime, self(), q_timeout),
   State#state{silent_timer_ref = NewQTimer, is_quiet = true}.


maybe_trigger_restart_timer(State = #state{trigger_on_value = true}) ->
   State;
maybe_trigger_restart_timer(State) ->
   restart_timer(State).

maybe_restart_timer(State = #state{is_quiet = true}) ->
   State;
maybe_restart_timer(State) ->
   restart_timer(State).

restart_timer(State = #state{timer_ref = TRef, timeout = Timeout}) ->
%%   lager:notice("(re)start timer"),
   catch erlang:cancel_timer(TRef),
   NewTimer = erlang:send_after(Timeout, self(), timeout),
   State#state{timer_ref = NewTimer}.
