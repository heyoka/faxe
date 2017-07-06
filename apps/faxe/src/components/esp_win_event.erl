%% Date: 05.01.17 - 17:41
%% Ⓒ 2017 heyoka
-module(esp_win_event).
-author("Alexander Minichmair").

-behaviour(df_component).

-include("faxe.hrl").

%% API
-export([init/3, process/3, handle_info/2, options/0]).

options() ->
   [{period, integer, 12}, {every, integer, 4}, {module, atom, c_noop}, {field, atom, <<"val">>}].

init(NodeId, _Inputs, #{period := Period, every := Every, module := AggMod, field := F}) ->
   io:format("~p init:node~n",[NodeId]),
   State = #esp_window{agg_mod = AggMod, every = Every, period = Period, agg_fields = F},
   {ok, all, State}.


process(_Inport, #data_point{ts = Ts} = Point,
    State=#esp_window{agg_fields = Field, stats = #esp_win_stats{} = Stats} ) ->

   Val = flowdata:field(Point, Field),

   NewStats = accumulate({Ts, Val, Point}, Stats),
   EvictState = maybe_evict(State#esp_window{stats = NewStats}),

   maybe_emit(EvictState).

handle_info(Request, State) ->
   io:format("~p request: ~p~n", [State, Request]),
   {ok, State}.


%%%===================================================================
%%% Internal functions
%%%===================================================================

accumulate({Ts, Val, Event}, Stats = #esp_win_stats{events = {[], [], []}, count = C, at = At}) ->
   NewStats = Stats#esp_win_stats{events = {[Ts], [Val], [Event]}, count = C+1, at = At+1 },
   NewStats;
accumulate({Ts, Val, Event}, Stats = #esp_win_stats{events = {Tss, Vals, Evs}, count = C, at = At}) ->
   NewEvents = {Tss++[Ts], Vals++[Val], Evs++[Event]},
   NewStats = Stats#esp_win_stats{events = NewEvents, count = C+1, at = At+1},
   NewStats.

maybe_emit(Win = #esp_window{agg_mod = AggMod, agg_fields = As, agg = AggState, every = Count,
                                                   stats = #esp_win_stats{count = Count, events = {Tss, Vals, _}} = Stats}) ->
   lager:info("emit when count is : ~p",[Count]),
   Result = c_agg:call({Tss, Vals}, AggMod, AggState, As),
   lager:info("~n~p emitting : ~p",[?MODULE, {Result, length(Result#data_batch.points)}]),
   {emit, Result, Win#esp_window{stats = Stats#esp_win_stats{count = 0}}}
;
maybe_emit(Win = #esp_window{}) ->
   {ok, Win}.

maybe_evict(Win = #esp_window{period = Period,
      stats = #esp_win_stats{at = At, events = Events} = Stats}) when At == Period+1 ->
   {Keep, {_ETs, _EVal, _EEv}} = evict(Events),
   Win#esp_window{stats = Stats#esp_win_stats{at = Period, events = Keep}};
maybe_evict(Win = #esp_window{}) ->
   Win.

evict({[HTs|Ts], [HVals|Vals], [HEvents|Events]}) ->
   lager:info("EVICT ~p", [HVals]),
   {{Ts, Vals, Events}, {HTs, HVals, HEvents}}.
