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
-export([init/3, process/3, options/0, handle_info/2, check_options/0, init/4, format_state/1]).


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
   repeat_interval = undefined,
   last_ts = undefined,
   last_point,
   no_forward
}).

options() -> [
   {timeout, duration}, %%
   {fields, string_list, []},
   {field_values, list, []},
   {silent_time, duration, <<"0ms">>}, %% for this amount of time no timeout is triggered
   {repeat_last, is_set},
   %% deprecated
   {repeat_with_new_ts, bool, true},
   %% add this amount of time, when repeating a message
   {repeat_interval, duration, undefined},
   {trigger_on_value, is_set},
   {no_forward, is_set}
].

check_options() -> [{same_length, [fields, field_values]}].

format_state(#state{last_ts = LastTs, last_point = LastPoint}) ->
   #{last_ts => LastTs, last_point => LastPoint}.

init(NodeId, Ins, Opts, #node_state{state = #{last_ts := LastTs, last_point := LastPoint}}) ->
   {ok, Mode, InitState} = init(NodeId, Ins, Opts),
   NewState = InitState#state{last_point = LastPoint, last_ts = LastTs, is_quiet = false},
   {ok, Mode, NewState}.

init(NodeId, _Ins,
    #{timeout := Timeout0, fields := Fields, repeat_last := Repeat, no_forward := NoForward,
       trigger_on_value := Trigger, field_values := Vals, silent_time := QTime0,
       repeat_with_new_ts := NewTs, repeat_interval := TsInterval0}) ->


   Timeout = faxe_time:duration_to_ms(Timeout0),
   QTimeout = faxe_time:duration_to_ms(QTime0),
   TsInterval = case TsInterval0 of undefined -> undefined; _ -> faxe_time:duration_to_ms(TsInterval0) end,
   State =
      #state{timeout = Timeout, fields = Fields, repeat_last = Repeat, no_forward = NoForward,
         repeat_with_new_ts = NewTs, node_id = NodeId, field_vals = Vals, silent_time = QTimeout,
         trigger_on_value = Trigger, repeat_interval = TsInterval},

   {ok, true, maybe_trigger_restart_timer(State)}.

process(_In, Data=#data_point{ts = Ts}, State = #state{no_forward = true}) ->
   NewState = State#state{last_point = Data, last_ts = Ts},
   {ok, maybe_restart_timer(NewState)};
process(_In, Data=#data_point{ts = Ts}, State = #state{}) ->
   NewState = State#state{last_point = Data, last_ts = Ts},
   {emit, Data, maybe_restart_timer(NewState)}.

handle_info(q_timeout, State = #state{}) ->
%%   lager:info("silent timeout is up"),
   {ok, restart_timer(State#state{is_quiet = false, silent_timer_ref = undefined})};

handle_info(timeout, State = #state{}) ->
   {OutPoint, NewState0} = build_message(State),
   NewState = maybe_start_qtimer(NewState0),
   {emit, OutPoint, maybe_restart_timer(NewState)};
handle_info(_R, State) ->
   {ok, State}.


build_message(
    S=#state{repeat_last = true, repeat_interval = Interval, last_point = P=#data_point{ts = Ts}, last_ts = LastTs})
      when is_integer(Interval) ->
   NewTs =
   case LastTs of
      undefined -> Ts + Interval;
      ATs -> ATs + Interval
   end,
   {P#data_point{ts = NewTs}, S#state{last_ts = NewTs}};
build_message(S=#state{repeat_last = true, repeat_with_new_ts = true, last_point = P=#data_point{}}) ->
   {P#data_point{ts = faxe_time:now()}, S};
build_message(S=#state{repeat_last = true, last_point = P=#data_point{}}) ->
   {P, S};
build_message(S=#state{field_vals = Vals, fields = Fields}) ->
   DataPoint = #data_point{ts = faxe_time:now()},
   {flowdata:set_fields(DataPoint, Fields, Vals), S}.

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
