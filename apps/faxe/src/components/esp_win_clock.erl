%% Date: 05.01.17 - 17:41
%% Ⓒ 2017 heyoka
-module(esp_win_clock).
-author("Alexander Minichmair").

-behaviour(df_component).

-include("faxe.hrl").

%% API
-export([init/3, process/3, handle_info/2, options/0]).

options() ->
   [{period, binary}, {every, binary}, {module, atom, c_noop}, {field, atom, <<"val">>}, {align, is_set}].

init(NodeId, _Inputs, #{period := Period, every := Every, module := AggMod, field := F, align := Align}) ->
   NUnit =
      case Align of
         false -> false;
         true -> faxe_time:binary_to_duration(Every)
      end,
   io:format("~p init:node~n",[NodeId]),
   Every1 = faxe_time:duration_to_ms(Every),
   Per = faxe_time:duration_to_ms(Period),
   State = #esp_window{period = Per, every = Every1, agg_mod = AggMod, agg_fields = F},
   %% window - tick
   erlang:send_after(State#esp_window.tick_time, self(), tick),
   {ok, all, State#esp_window{stats = #esp_win_stats{} , align = NUnit}}.


process(_Inport, #data_point{ts = Ts} = Point, State=#esp_window{agg_fields = Field}) ->
   Val = flowdata:field(Point, Field),
   NewState = accumulate({Ts, Val, Point}, State),
   {ok, NewState}
.


handle_info(tick, State=#esp_window{tick_time = Tick}) ->
   NewState = tick(State),
   erlang:send_after(Tick, self(), tick),
   {ok, NewState}
;
handle_info(Request, State) ->
   io:format("~p request: ~p~n", [State, Request]),
   {ok, State}.


%%%===================================================================
%%% Internal functions
%%%===================================================================
accumulate({NewTs, NewVal, NewEvent}, State = #esp_window{log = [], align = Align,
      stats = #esp_win_stats{events = {[], [], []} }=Stats}) ->

   NewTime = faxe_time:now(),
   NewStats = Stats#esp_win_stats{events = {[NewTs], [NewVal], [NewEvent]}, mark = new_ts(NewTs, Align)},
   State#esp_window{log = [NewTime], stats = NewStats};
accumulate({NewTs, NewVal, NewEvent}, State = #esp_window{log = Log,
      stats = #esp_win_stats{events = {Tss, Vals, Evs} }=Stats}) ->

   NewTime = faxe_time:now(),
   NewEvents = {Tss++[NewTs], Vals++[NewVal], Evs++[NewEvent]},
   NewStats = Stats#esp_win_stats{events = NewEvents},
   State#esp_window{log = Log ++ [NewTime], stats = NewStats}.

tick(State = #esp_window{stats = #esp_win_stats{events = {[],[],[]}}}) ->
   State;
tick(State = #esp_window{agg_mod = _Module, log = Log, every = Every,
      stats = #esp_win_stats{events = Events, mark = Mark} = Stats, period = Interval}) ->
   NewAt = faxe_time:now(),
   lager:info("Tick AT: ~p", [faxe_time:to_date(NewAt)]),
   % on a tick, we check for sliding out old events
   {KeepLog, Keep, _Evict} = evict(Log, Events, NewAt, Interval),

   {NewState, NewStats} = case (NewAt - Mark) >= Every of
                             true -> lager:notice("NewMARK: ~p",[faxe_time:to_date(Mark+Every)]),
                                       {emit(State), Stats#esp_win_stats{mark = Mark+Every}};
                             false -> {State, Stats}
                          end,
   NewState#esp_window{stats = NewStats#esp_win_stats{events = Keep, at = NewAt}, log = KeepLog}.

%% emit window result(s)
emit(#esp_window{agg_mod = AggMod, stats = #esp_win_stats{events = {Tss, Vs, _E}} , agg = AggState, agg_fields = As} = State) ->
   Res = c_agg:call({Tss, Vs}, AggMod, AggState, As),
   lager:warning("~p emitting: ~p",[?MODULE, {Res, length(Res#data_batch.points)}]),
   dataflow:emit(Res),
   State.


%%%% filter list and evict old entries, with early abandoning
%%%% since the list is ordered by time, the abandoning will work as expected
%%%% as soon as a timestamp falls into the bounderies of the window, the loop is stopped
%%%% and the rest of the list is returned, as is for Keep
-spec evict(list(), window_events(), non_neg_integer(), non_neg_integer()) -> {window_events(), list()}.
evict(Log, {Ts, Vals, Events}, At, Interval) ->
   {KeepLog, Evict} = win_util:split(Log, At - Interval),

   {KeepLog, {win_util:sync(Ts, Evict), win_util:sync(Vals, Evict), win_util:sync(Events, Evict)}, Evict}
.

new_ts(Ts, false) ->
   Ts;
new_ts(Ts, Align) ->
   faxe_time:align(Ts, Align).
