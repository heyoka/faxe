%% Date: 15.07.2019 - 09:55
%% Ⓒ 2019 heyoka
%% @doc
%% This node takes a list of lambda expressions representing different states.
%% It will emit values only after each state has evaluated as true in the given order and, for each step in the sequence
%% within the corresponding timeout.
%% You must define a timeout for every state transition with the 'within' parameter.
%%
%% If a timeout occurs at any point the sequence will be reset and started from the first expression again.
%%
%% Note that the sequence timeouts start after the first datapoint has satisfied the first lambda expression.
%% Therefore, if 3 lambda states are given, only 2 durations for the 'within' parameter can be defined.
%%
%% With the 'strict' parameter the sequence of states must be met exactly without any datapoints coming in,
%% that do not satisfy the current state expression.
%% Normally this would not reset the sequence of evaluation, in this mode, it will.
%%
%% On a successful evaluation of the whole sequence,
%% the node will simply output the last value, that completed the sequence.
%%
%% The state_sequence node can be used with one or many input nodes.
%%
-module(esp_state_sequence).
-author("Alexander Minichmair").

%% API
-behavior(df_component).

-include("faxe.hrl").
%% API
-export([init/3, process/3, options/0, handle_info/2]).

-record(state, {
   node_id,
   lambdas,
   durations,
   current_index,
   tref,
   is_strict = false

}).

options() -> [
   {states, lambda_list},
   {within, duration_list},
   {strict, is_set, false},
   {output, string, <<"last">>}
].

init(_NodeId, _Ins, #{states := Lambdas, within := DurList0, strict := Strict}) ->
   DurList = [faxe_time:duration_to_ms(Dur) || Dur <- DurList0],
   {ok, all, #state{lambdas = Lambdas, durations = DurList, current_index = 1, is_strict = Strict}}.

process(_In, #data_batch{points = _Points} = _Batch, _State = #state{lambdas = _Lambda}) ->
   {error, not_implemented};

process(_Inport, #data_point{} = Point, State = #state{current_index = Index, lambdas = Lambdas}) ->
   lager:notice("in on port :~p~n~p",[_Inport, lager:pr(Point, ?MODULE)]),
   case exec(Point, lists:nth(Index, Lambdas)) of
      true -> eval_true(Point, State);
      false -> eval_false(State)
   end.


handle_info(state_timeout, State = #state{}) ->
   lager:warning("state_timeout when index: ~p",[State#state.current_index]),
   {ok, reset(State)}.

exec(Point, LFun) -> faxe_lambda:execute_bool(Point, LFun).

eval_true(Point, State = #state{current_index = Current, tref = TRef, lambdas = Lambdas, durations = DurList}) ->
   lager:info("ok, go to next state from (~p)" , [Current]),
   catch (erlang:cancel_timer(TRef)),
   case Current == length(Lambdas) of
      true -> lager:notice("done, reset !"),
         {emit, Point, reset(State)};
      false ->
         lager:notice("start timeout: ~p", [lists:nth(Current, DurList)]),
         Timer = erlang:send_after(lists:nth(Current, DurList), self(), state_timeout),
         {ok, State#state{current_index = Current + 1, tref = Timer}}
   end.

eval_false(State = #state{is_strict = true}) ->
   lager:notice("strict, no match: reset!"),
   {ok, reset(State)};
eval_false(State) ->
   {ok, State}.

reset(State = #state{tref = Timer}) ->
   catch (erlang:cancel_timer(Timer)),
   State#state{current_index = 1, tref = undefined}.




