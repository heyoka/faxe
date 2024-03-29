%% Date: 27.01.20 - 18:38
%% Ⓒ 2020 heyoka
%% @doc
%% emits a point, if there is no message coming in for the given amount of time
%%
%% A timeout will be started on an explicit trigger:
%% When a lambda expression is given for parameter timeout_trigger, this expression must evaluate as true
%% to start (and after a timeout has ocurred to restart) a timeout.
%%
%% If no lambda expression is given for the timeout_trigger, the trigger is any datapoint coming in on port 1.
%%
%% A new trigger does not restart a running timeout.
%% After a timeout occurred, the node waits for a new trigger to come in before it starts a new timeout.
%%
%% After a timeout is started the node waits for data coming in,
%% that either does not satisfy the trigger expression(when a lambda expression is given for
%% the timeout_trigger parameter) or is coming in on any port except port 1.
%%
%% Data for the outgoing data-point can be defined with the fields and field_values parameters.
%% This node can have any number of input-nodes.
%% @end
-module(esp_triggered_timeout).
-author("Alexander Minichmair").

%% API
-behavior(df_component).

-include("faxe.hrl").
%% API
-export([init/3, process/3, options/0, handle_info/2, check_options/0, wants/0, emits/0]).


-record(state, {
   node_id,
   timeout,
   fields,
   field_vals,
   cancel_fields,
   cancel_field_vals,
   timer_ref,
   trigger_port = 1,
   trigger_fun,
   cancel_fun
}).

options() -> [
   {timeout, duration}, %%
   {fields, string_list, []},
   {field_values, list, []},

   {cancel_fields, string_list, []},
   {cancel_field_values, list, []},
   %{timeout_trigger_port, integer, 1}, %% maybe later
   {timeout_trigger, lambda, undefined},
   {cancel_trigger, lambda, undefined}
].

check_options() -> [{same_length, [fields, field_values]}].

wants() -> point.
emits() -> point.

init(NodeId, _Ins,
    #{timeout := Timeout0, fields := Fields, field_values := Vals,
       cancel_fields := CFields, cancel_field_values := CVals,
       timeout_trigger := Lambda, cancel_trigger := CancelLambda
%%       ,
%%       timeout_trigger_port := TriggerPort
    }) ->
   Timeout = faxe_time:duration_to_ms(Timeout0),

   State =
      #state{timeout = Timeout,
         fields = Fields, field_vals = Vals,
         cancel_fields = CFields, cancel_field_vals = CVals,
         trigger_fun = Lambda, cancel_fun = CancelLambda,
         node_id = NodeId},

   {ok, all, State}.

%% if we have a trigger_fun, we do not check the cancel_fun, so they exclude each other
process(InPort, Item, State = #state{trigger_fun = undefined, trigger_port = InPort}) ->
   NewState = maybe_start_timer(State), %% ok we hit the timeout trigger
   {ok, cancel_fun(Item, NewState)};
process(_In, _Item, State = #state{trigger_fun = undefined}) ->
   {ok, cancel_timer(State)};
process(_In, P = #data_point{}, State = #state{trigger_fun = Fun}) ->
   NewState =
      case (catch faxe_lambda:execute(P, Fun)) of
         true -> maybe_start_timer(State); %% ok we hit the timeout trigger
         _ -> cancel_timer(State) %% cancel the timeout
      end,
   {ok, NewState}.

cancel_fun(_P, State = #state{cancel_fun = undefined}) ->
   State;
cancel_fun(P, State = #state{cancel_fun = Fun}) ->
   case (catch faxe_lambda:execute(P, Fun)) of
      true -> lager:notice("cancel_fun triggered !!!"),cancel_timer(State); %% cancel the timeout
      _ -> State
   end.

handle_info(timeout, State = #state{}) ->
   {emit, {1, build_message(State)}, State#state{timer_ref = undefined}};
handle_info(_R, State) ->
   {ok, State}.

build_message(#state{field_vals = Vals, fields = Fields}) ->
   DataPoint = #data_point{ts = faxe_time:now()},
   flowdata:set_fields(DataPoint, Fields, Vals).

build_cancel_message(#state{cancel_field_vals = Vals, cancel_fields = Fields}) ->
   DataPoint = #data_point{ts = faxe_time:now()},
   flowdata:set_fields(DataPoint, Fields, Vals).

maybe_start_timer(State = #state{timer_ref = undefined}) ->
   lager:info("start new timeout"),
   restart_timer(State);
maybe_start_timer(State = #state{timer_ref = TRef}) when is_reference(TRef) ->
   State.

cancel_timer(State = #state{timer_ref = TRef}) ->
   lager:info("cancel timeout"),
   catch erlang:cancel_timer(TRef),
   dataflow:emit(build_cancel_message(State)),
   State#state{timer_ref = undefined}.

restart_timer(State = #state{timer_ref = TRef, timeout = Timeout}) ->
   catch erlang:cancel_timer(TRef),
   NewTimer = erlang:send_after(Timeout, self(), timeout),
   State#state{timer_ref = NewTimer}.
