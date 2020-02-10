%% Date: 15.07.2019 - 09:55
%% Ⓒ 2019 heyoka
%% @doc
%%
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
   tref

}).

options() -> [
   {states, lambda_list},
   {within, duration_list, [<<"1s">>]},
   {output, string, <<"last">>}
].

init(_NodeId, _Ins, #{states := Lambdas, within := DurList0}) ->
   DurList = [faxe_time:duration_to_ms(Dur) || Dur <- DurList0],
   {ok, all, #state{lambdas = Lambdas, durations = DurList, current_index = 1}}.

process(_In, #data_batch{points = _Points} = _Batch, _State = #state{lambdas = _Lambda}) ->
   {error, not_implemented};

process(_Inport, #data_point{} = Point, State = #state{}) ->
   lager:notice("in on port :~p~n~p",[_Inport, lager:pr(Point, ?MODULE)]),
   eval_state(Point, State).


handle_info(state_timeout, State = #state{}) ->
   lager:warning("state_timeout when index: ~p",[State#state.current_index]),
   {ok, State#state{current_index = 1}}.


eval_state(Point, State=#state{current_index = Index, lambdas = Lambdas}) ->
   case exec(Point, lists:nth(Index, Lambdas)) of
      true -> lager:info("ok, go to next state"),
            case next(State) of
                 {emit, NewState} -> {emit, Point, NewState};
                 {false, NewState} -> {ok, NewState}
              end;
      false -> lager:info("not what we are waiting for .... "),
         {ok, State}
   end.

exec(Point, LFun) -> faxe_lambda:execute(Point, LFun).

next(State=#state{current_index = Current, tref = TRef, lambdas = Lambdas, durations = DurList}) ->
   catch (erlang:cancel_timer(TRef)),
   {Mode, NewTRef, NewIndex} =
   case Current == length(Lambdas) of
      true -> lager:notice("done, reset !"),
         {emit, undefined, 1};
      false ->
         lager:notice("start timeout: ~p",[lists:nth(Current, DurList)]),
         Timer = erlang:send_after(lists:nth(Current, DurList), self(), state_timeout),
         {false, Timer, Current + 1}
   end,
   {Mode, State#state{tref = NewTRef, current_index = NewIndex}}.



