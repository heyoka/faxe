%% Date: 19.12.16 - 09:55
%% Ⓒ 2016 heyoka
-module(esp_agg_js).
-author("Alexander Minichmair").

-include("faxe.hrl").
%%noinspection ErlangUndefinedBehaviour
%% API
-export([init/1, accumulate/2, compensate/2, emit/2]).

-define(FN_ACC, <<"accumulate">>).
-define(FN_COMP, <<"compensate">>).
-define(FN_EMIT, <<"emit">>).

-define(JS_ACCUMULATE, <<"var accumulate = function(value, state){ return value + 1; }">>).
-define(JS_COMPENSATE, <<"var compensate = function(value, state){ return value - 1; }">>).
-define(JS_EMIT, <<"var emit = function(all_win_values, state){ return all_win_values; }">>).

-record(state, {
   js_state,
   fn_acc, fn_comp, fn_emit,
   js_driver}).


init({AccFn, CompFn, EmitFn}) ->
   {ok, Js} = js_driver:new(),
   {ok, #state{js_driver = Js,
      fn_acc = reg_js_func(Js, AccFn), fn_comp = reg_js_func(Js, CompFn),
      fn_emit = reg_js_func(Js, EmitFn)}}
;
%% for testing purposes
init(_) ->
   init({?JS_ACCUMULATE, ?JS_COMPENSATE, ?JS_EMIT}).

accumulate(_Event, #state{fn_acc = undefined}=State) -> {ok, State};
accumulate(Event, #state{js_state = JsState, js_driver = Js}=State) ->
   {ok, NewJsState} = js:call(Js, ?FN_ACC, [Event, JsState]),
   {ok, State#state{js_state = NewJsState}}.

compensate(_Event, #state{fn_comp = undefined}=State) -> {ok, State};
compensate(Event, #state{js_state = JsState, js_driver = Js}=State) ->
   {ok, NewJsState} = js:call(Js, ?FN_COMP, [Event, JsState]),
   {ok, State#state{js_state = NewJsState}}.


emit(#esp_win_stats{events = {_Timestamps, Values, _Events}},
      #state{js_state = JsState, js_driver = Js}=State) ->
   {ok, Value} = js:call(Js, ?FN_EMIT, [Values, JsState]),
   {ok, Value, State}.


%%%%%%%%%%%%%%%%%% private %%%%%%%%%%%%%%%%%%%
reg_js_func(_Driver, undefined) -> undefined;
reg_js_func(Driver, Func) -> ok = js:define(Driver, Func), ok.


%%%%%%%%%%%%%%%% tests %%%%%%%%%%%%%%%%%%%%%%%
-ifdef(TEST).

basic_test() ->
   {ok, S0=#state{js_state = undefined}} = init({?JS_ACCUMULATE, ?JS_COMPENSATE, ?JS_EMIT}),
   {ok, S2=#state{js_state = JsState0}} = accumulate(13, S0),
   ?assertEqual(14, JsState0),
   {ok, S3=#state{js_state = JsState1}} = compensate(13, S2),
   ?assertEqual(12, JsState1),
   ?assertEqual(
      {ok, [1,2,3], S3}, emit(#esp_win_stats{events = {[], [1,2,3],[]}}, S3)
   ).

-endif.