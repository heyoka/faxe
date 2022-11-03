%%%-------------------------------------------------------------------
%%% @author heyoka
%%% @copyright (C) 2020, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 08. May 2020 18:27
%%%-------------------------------------------------------------------
-module(state_change).
-author("heyoka").

-include("faxe.hrl").

%% API
-export([
  new/1, new/2, process/2, get_duration/1, get_count/1, get_state/1,
  get_last_point/1, get_last_duration/1, get_last_count/1, get_last_enter_time/1, get_end/1]).

%%%%%%%%%%% known states %%%%%%%
-define(Init, init).
-define(In, in).
-define(Out, out).
%% just left the state
-define(Left, left).
%% means just entered
-define(Entered, entered).


-record(state_change, {
  state_name = ?Init,
  last_point = undefined,
  leave_point = undefined,
  enter_time,
  last_enter_time,
  state_count = -1,
  state_fun,
  last_duration = -1,
  last_state_count = -1,
  field_path
  }).


-spec new(function()) -> #state_change{}.
new(StateFun) when is_function(StateFun) ->
  new(StateFun, undefined).
new(StateFun, FieldPath) ->
  #state_change{state_fun = StateFun, field_path = FieldPath}.

-spec process(#state_change{}, #data_point{}) -> {ok, #state_change{}}.
process(State, Point = #data_point{}) ->
  execute(State, Point).

%% @doc get the current state-duration
-spec get_duration(#state_change{}) -> integer().
get_duration(#state_change{enter_time = undefined}) ->
  -1;
get_duration(#state_change{last_point = #data_point{ts = LastTs}, enter_time = T}) ->
  LastTs - T.
%% @doc get the current state-count
-spec get_count(#state_change{}) -> integer().
get_count(#state_change{state_count = Count}) ->
  Count.

%% @doc get the last total state-duration,
%% this is set on state-leave
-spec get_last_duration(#state_change{}) -> integer().
get_last_duration(#state_change{last_duration = undefined}) ->
  -1;
get_last_duration(#state_change{last_duration = Dur}) ->
  Dur.
%% @doc get the last total state-count,
%% this is set on state-leave
-spec get_last_count(#state_change{}) -> integer().
get_last_count(#state_change{last_state_count = C}) ->
  C.

%% @doc get the last time the state was entered,
%% this is set on state leave
-spec get_last_enter_time(#state_change{}) -> undefined | non_neg_integer().
get_last_enter_time(#state_change{last_enter_time = EnterTime}) ->
  EnterTime.

-spec get_state(#state_change{}) -> init|entered|in|out|left.
get_state(#state_change{state_name = State}) ->
  State.

-spec get_last_point(#state_change{}) -> #data_point{}|undefined.
get_last_point(#state_change{last_point = P}) ->
  P.

get_end(#state_change{leave_point = P}) ->
  P#data_point.ts.


execute(State = #state_change{}, Point) ->
  comp((catch exec(Point, State)), State, Point).
exec(Point, #state_change{state_fun = LFun}) when is_function(LFun) ->
  faxe_lambda:execute(Point, LFun);
exec(Point, #state_change{field_path = Field, state_fun = Value}) ->
  flowdata:field(Point, Field) == Value.

comp({_Error, _Reason}, _State,_P = #data_point{}) ->
  {error, error_evaluating_lambda_fun};
comp(true, State = #state_change{state_name = ?Init}, P = #data_point{}) ->
  %% ENTERING !!!
  enter(State, P);
comp(false, State = #state_change{state_name = ?Init}, #data_point{}) ->
  {ok, State#state_change{state_name = ?Out, state_count = -1}};
comp(true, State = #state_change{state_name = ?Entered}, P = #data_point{}) ->
  in_entered(State, P);
comp(true, State = #state_change{state_name = ?In}, P) ->
  in_entered(State, P);
comp(true, State = #state_change{state_name = Name}, P = #data_point{}) when Name == ?Out orelse Name == ?Left ->
  %% ENTERING !!!
  enter(State, P);
comp(false, State = #state_change{state_name = Name, state_count = C, enter_time = T, last_point = _LastP}, P)
  when Name == ?In orelse Name == ?Entered ->
  %% LEAVING !!!
  {ok, State#state_change{state_name = ?Left, state_count = -1,  last_state_count = C,
    last_duration = P#data_point.ts - T,  enter_time = undefined, last_enter_time = T, leave_point = P}};
comp(false, State = #state_change{state_name = Name}, _P) when Name == ?Out orelse Name == ?Left ->
  {ok, State#state_change{state_name = ?Out, last_point = undefined, enter_time = undefined, state_count = -1}}.

%% when StateFun gave true
in_entered(State=#state_change{state_count = C}, NewPoint) ->
  {ok, State#state_change{state_name = ?In, last_point = NewPoint, state_count = C+1}}.
enter(State=#state_change{}, P = #data_point{ts = Ts}) ->
  {ok, State#state_change{state_name = ?Entered, state_count = 1, last_point = P, enter_time = Ts}}.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-ifdef(TEST).
basic_test() ->

  State = state_change:new(fun(_E) -> true end),

  ?assertEqual(-1, state_change:get_count(State)),
  ?assertEqual(?Init, state_change:get_state(State)),
  %% ENTER
  P = #data_point{ts = 1234, fields = #{val => 1}},
  {ok, NewStateEnter} = comp(true, State, P),
  {ok, NewStateFalse} = comp(false, State, P),
  ?assertEqual(?Entered, state_change:get_state(NewStateEnter)),
  ?assertEqual(?Out, state_change:get_state(NewStateFalse)),
  ?assertEqual(-1, state_change:get_last_count(NewStateEnter)),
  ?assertEqual(-1, state_change:get_last_duration(NewStateEnter)),
  ?assertEqual(1234, NewStateEnter#state_change.enter_time),
  %% IN
  NextP = P#data_point{ts = 2235},
  {ok, NewStateIn} = comp(true, NewStateEnter, NextP),
  ?assertEqual(?In, state_change:get_state(NewStateIn)),
  ?assertEqual(2, state_change:get_count(NewStateIn)),
  ?assertEqual(1001, state_change:get_duration(NewStateIn)),
  ?assertEqual(1234, NewStateIn#state_change.enter_time),
  %% IN1
  NextP1 = P#data_point{ts = 3236},
  {ok, NewStateIn1} = comp(true, NewStateIn, NextP1),
  ?assertEqual(?In, state_change:get_state(NewStateIn1)),
  ?assertEqual(3, state_change:get_count(NewStateIn1)),
  ?assertEqual(2002, state_change:get_duration(NewStateIn1)),
  ?assertEqual(NextP1, NewStateIn1#state_change.last_point),
  ?assertEqual(1234, NewStateIn1#state_change.enter_time),
  %% LEAVE
  {ok, NewStateLeft} = comp(false, NewStateIn1, NextP),
  ?assertEqual(?Left, state_change:get_state(NewStateLeft)),
  ?assertEqual(3, state_change:get_last_count(NewStateLeft)),
  ?assertEqual(1001, state_change:get_last_duration(NewStateLeft)),
  ?assertEqual(-1, state_change:get_count(NewStateLeft)),
  ?assertEqual(-1, state_change:get_duration(NewStateLeft)),
  ?assertEqual(NextP1, NewStateLeft#state_change.last_point),
  ?assertEqual(undefined, NewStateLeft#state_change.enter_time),
  ?assertEqual(1234, state_change:get_last_enter_time(NewStateLeft)),
  %% OUT
  {ok, NewStateOut} = comp(false, NewStateLeft, NextP),
  ?assertEqual(?Out, state_change:get_state(NewStateOut)),
  ?assertEqual(-1, state_change:get_count(NewStateOut)),
  ?assertEqual(-1, state_change:get_duration(NewStateOut)),
  ?assertEqual(undefined, state_change:get_last_point(NewStateOut)),
  %% REENTER
  NextP2 = NextP#data_point{ts = 4237},
  {ok, NewStateReentered} = comp(true, NewStateOut, NextP2),
  ?assertEqual(?Entered, state_change:get_state(NewStateReentered)),
  ?assertEqual(1, state_change:get_count(NewStateReentered)),
  ?assertEqual(0, state_change:get_duration(NewStateReentered)),
  ?assertEqual(NextP2, state_change:get_last_point(NewStateReentered))
.

process_test() ->
  P = #data_point{ts = 1234, fields = #{val => 1}},
  LambdaString = "Val == 1",
  Lambda = lambda_tests:lambda_helper(LambdaString, ["val"]),
  State = state_change:new(Lambda),
  {ok, NewState} = state_change:process(State, P),
  ?assertEqual(?Entered, state_change:get_state(NewState)).

process_err_test() ->
  P = #data_point{ts = 1234, fields = #{val2 => 1}},
  LambdaString = "Val + 1 > 1",
  Lambda = lambda_tests:lambda_helper(LambdaString, ["val"]),
  State = state_change:new(Lambda),
  ?assertEqual({error, error_evaluating_lambda_fun}, state_change:process(State, P)).

-endif.