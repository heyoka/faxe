%% Date: 05.01.17 - 17:41
%% Ⓒ 2017 heyoka
%% @doc this window-type has wall-clock timing
%%
-module(esp_win_clock_q).
-author("Alexander Minichmair").

-behaviour(df_component).

-include("faxe.hrl").

%% API
-export([init/3, process/3, handle_info/2, options/0]).

-define(TICK_INTERVAL, 500).

-record(state, {
   period,
   every,
   align,
   mark,
   fill_period,
   tick_time,
   window,
   log = []
}).

options() ->
   [{period, binary}, {every, binary}, {align, is_set, true}, {fill_period, is_set, false}].

init(NodeId, _Inputs, #{period := Period, every := Every, align := Align, fill_period := Fill}) ->
   NUnit =
      case Align of
         false -> false;
         true -> faxe_time:binary_to_duration(Every)
      end,
   io:format("~p init:node~n",[NodeId]),
   Every1 = faxe_time:duration_to_ms(Every),
   Per = faxe_time:duration_to_ms(Period),
   State =
      #state{period = Per, every = Every1, align = NUnit,
         fill_period = Fill, tick_time = ?TICK_INTERVAL, window = queue:new()},
   %% window - tick
   erlang:send_after(State#esp_window.tick_time, self(), tick),
   {ok, all, State}.


process(_Inport, #data_point{ts = _Ts} = Point, State=#state{}) ->
   NewState = accumulate(Point, State),
   {ok, NewState}
.


handle_info(tick, State=#state{tick_time = Tick}) ->
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
accumulate(Point=#data_point{ts = NewTs}, State = #state{log = [], mark = undefined, align = Align, window = Win}) ->
   NewTime = faxe_time:now(),
   State#state{log = [NewTime], window = queue:in(Point, Win), mark = new_ts(NewTs, Align)};
accumulate(Point=#data_point{}, State = #state{log = Log, window = Win}) ->
   NewTime = faxe_time:now(),
   State#state{log = Log ++ [NewTime], window = queue:in(Point, Win)}.

tick(State = #state{log = []}) ->
   State;
tick(State = #state{log = Log, every = Every,mark = Mark , period = Interval, window = Window}) ->
   NewAt = faxe_time:now(),
   lager:info("Tick AT: ~p", [faxe_time:to_date(NewAt)]),
   % on a tick, we check for sliding out old events
   {KeepLog, NewWindow, _HasEvicted} = evict(Log, Window, NewAt, Interval),

   NewState =
      case (NewAt - Mark) >= Every of
         true -> lager:notice("NewMARK: ~p", [faxe_time:to_date(Mark + Every)]),
            Batch = #data_batch{points = queue:to_list(NewWindow)},
            dataflow:emit(Batch),
            State#state{mark = Mark + Every};
         false -> State
      end,
   NewState#state{log = KeepLog, window = NewWindow}.

%%%% filter list and evict old entries, with early abandoning
%%%% since the list is ordered by time, the abandoning will work as expected
%%%% as soon as a timestamp falls into the bounderies of the window, the loop is stopped
%%%% and the rest of the list is returned, as is for Keep
-spec evict(list(), window_events(), non_neg_integer(), non_neg_integer()) -> {window_events(), list()}.
evict(Log, Window, At, Interval) ->
   {KeepLog, Evict} = win_util:split(Log, At - Interval),
   {KeepLog, win_util:sync_q(Window, Evict), length(Evict)}
.

new_ts(Ts, false) ->
   Ts;
new_ts(Ts, Align) ->
   faxe_time:align(Ts, Align).
