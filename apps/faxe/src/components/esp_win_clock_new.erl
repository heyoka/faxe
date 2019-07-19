%% Date: 05.01.17 - 17:41
%% Ⓒ 2019 heyoka
%% @doc
%% this window-type has wall-clock timing, timestamps contained in incoming events are ignored
%%
%% when the align option is true, window boundaries are aligned according to the every option, this means
%% when every is 5s and an event comes into the window at time 15:03:27, this event will be member of the window
%% that starts at 15:03:25, otherwise the window would start at 15:03:27
%%
%% when the fill_period option is given, the window will not emit before "period" time has elapsed
%%
-module(esp_win_clock_new).
-author("Alexander Minichmair").

-behaviour(df_component).

-include("faxe.hrl").

%% API
-export([init/3, process/3, handle_info/2, options/0]).

-record(state, {
   period,
   every,
   align,
   next_emit,
   fill_period,
   window,
   log = [],
   has_emitted = false
}).

options() ->
   [{period, binary}, {every, binary}, {align, is_set, false}, {fill_period, is_set, false}].

init(NodeId, _Inputs, #{period := Period, every := Every, align := Align, fill_period := Fill} =P) ->
   NUnit =
      case Align of
         false -> false;
         true -> faxe_time:binary_to_duration(Every)
      end,
   io:format("~p init:node~nparams: ~p",[NodeId, P]),
   Every1 = faxe_time:duration_to_ms(Every),
   Per = faxe_time:duration_to_ms(Period),
   State =
      #state{period = Per, every = Every1, align = NUnit,
         fill_period = Fill, window = queue:new()},

   {ok, all, State}.


process(_Inport, #data_point{ts = _Ts} = Point, State=#state{}) ->
   NewState = accumulate(Point, State),
   {ok, NewState}
.

handle_info(emit_now, State=#state{}) ->
   lager:info("EMIT!"),
   NewState = emit(State),
   emit_at(NewState#state.next_emit),
   {ok, NewState}
;
handle_info(Request, State) ->
   io:format("~p request: ~p~n", [State, Request]),
   {ok, State}.


%%%===================================================================
%%% Internal functions
%%%===================================================================
%% the window starts running on the first message that comes in
accumulate(Point=#data_point{ts = NewTs},
    State = #state{log = [], align = Align, window = Win, every = Every, period = Period}) ->

   Now = faxe_time:now(),
   WinStart = new_ts(Now, Align),
   EmitAt = WinStart + Every,
   WinEnd = WinStart + Period,
   lager:notice("NOW: ~p ~nInitial win_start is at: ~p (win_end: ~p)  ~n while data_points timestamp is at : ~p",
      [faxe_time:to_htime(Now), faxe_time:to_htime(WinStart),faxe_time:to_htime(WinEnd), faxe_time:to_htime(NewTs)]),
   emit_at(EmitAt),
   State#state{log = [Now], window = queue:in(Point, Win), next_emit = EmitAt};
accumulate(Point=#data_point{}, State = #state{log = Log, window = Win}) ->
   NewTime = faxe_time:now(),
   State#state{log = Log ++ [NewTime], window = queue:in(Point, Win)}.


emit(State = #state{log = []}) ->
   State;
emit(State = #state{log = Log, next_emit = NextEmit, period = Interval, window = Window,
   fill_period = Fill, has_emitted = Emitted, align = Align, every = Every}) ->

   Now = faxe_time:now(),
   NewAt = new_ts(Now, Align), %% should be equal to NextEmit at this point
   lager:info("Emit AT: ~p (unaligned: ~p) should be : ~p",
      [faxe_time:to_htime(NewAt), faxe_time:to_htime(Now), faxe_time:to_htime(NextEmit)]),
   % on a tick, we check for sliding out old events
   {KeepLog, NewWindow, HasEvicted} = evict(Log, Window, NewAt, Interval),
   NewState =
      case (Fill == false) orelse (Fill == true andalso (Emitted == true orelse HasEvicted == true)) of
         true ->
            Batch = #data_batch{points = queue:to_list(NewWindow)},
            lager:warning("~n  period: ~p emitting: ~p",[Interval, length(Batch#data_batch.points)]),
            dataflow:emit(Batch),
            State#state{has_emitted = true};
         false -> State
      end,
   NewState#state{next_emit = NextEmit + Every, log = KeepLog, window = NewWindow}.

%%%% filter list and evict old entries, with early abandoning
%%%% since the list is ordered by time, the abandoning will work as expected
%%%% as soon as a timestamp falls into the bounderies of the window, the loop is stopped
%%%% and the rest of the list is returned, as is for Keep
-spec evict(list(), window_events(), non_neg_integer(), non_neg_integer()) -> {window_events(), list()}.
evict(Log, Window, At, Interval) ->
   {KeepTimestamps, Evict} = win_util:split(Log, At-Interval),
   lager:info("evict: [~p] ~p~n keep [~p]: ~p",[length(Evict), [faxe_time:to_date(E) || E <- Evict],
      length(KeepTimestamps), [faxe_time:to_date(T) || T <- KeepTimestamps]]),
   {KeepTimestamps, win_util:sync_q(Window, Evict), length(Evict) > 0}
.

new_ts(Ts, false) ->
   Ts;
new_ts(Ts, Align) ->
   faxe_time:align(Ts, Align).

emit_at(Timestamp) ->
   Now = faxe_time:now(),
   Time = Timestamp - Now,
   lager:notice("NEXT EMIT at: ~p", [faxe_time:to_date(Timestamp)]),
   erlang:send_after(Time, self(), emit_now).