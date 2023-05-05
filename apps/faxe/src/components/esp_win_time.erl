%% Date: 05.01.17 - 17:41
%% Ⓒ 2017 heyoka
%% @doc window which refers it's timing to the timestamp contained in the incoming data-items
%% rewrite with queue module instead of lists
%% @todo setup timeout where points get evicted even though there are timestamps missing ?
%%
-module(esp_win_time).
-author("Alexander Minichmair").

-behaviour(df_component).

-include("faxe.hrl").

%% API
-export([init/3, process/3, handle_info/2, options/0, wants/0, emits/0, init/4]).

-record(state, {
   every,
   period,
   window :: queue:queue(),
   ts_list = [], %% list of inserted Timestamps
   at,
   mark,
   fill_period,
   has_emitted = false %% false as long as the window never has emitted values
}).

options() ->
   [
      {period, duration, undefined},
      {every, duration},
      {fill_period, is_set}
   ].

wants() -> point.
emits() -> batch.

init(_NodeId, _Inputs, #{period := Period, every := Every, fill_period := Fill}) ->
   Ev = faxe_time:duration_to_ms(Every),
   Per = case Period of undefined -> Ev; _ ->  faxe_time:duration_to_ms(Period) end,
   %% fill_period does not make sense, if every is less than period
   DoFill = (Fill == true) andalso (Per > Ev),
   State = #state{period = Per, every = Ev, fill_period = DoFill, window = queue:new()},
   {ok, true, State}.

%% init with persisted state
init(_NodeId, _Inputs, _Opts, #node_state{state = PState}) ->
   {ok, true, PState}.


process(_Inport, #data_point{} = Point, State=#state{} ) ->
   State1 = tick(State),
   NewState = accumulate(Point, State1),
   {ok, NewState}.

handle_info(_Request, State) ->
   {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

accumulate(Point = #data_point{ts = Ts}, State = #state{mark = undefined}) ->
   accumulate(Point, State#state{mark = Ts});
accumulate(Point = #data_point{ts = Ts}, State = #state{window = Win, ts_list = TsList}) ->
   State#state{at = Ts, ts_list = TsList++[Ts], window = queue:in(Point, Win)}.

tick(State = #state{mark = undefined}) ->
   State;
tick(State = #state{at = At, window = Win, period = Interval, ts_list = TsList}) ->

   {KeepTsList, NewWindow, HasEvicted} = evict(TsList, Win, At, Interval),
   NewAt = lists:last(KeepTsList),
   case check_emit(NewAt, State, (HasEvicted orelse State#state.has_emitted)) of
      true ->
         Batch = #data_batch{points = queue:to_list(NewWindow), start = At},
%%         lager:warning("~n ~p emitting: ~p",[?MODULE, length(Batch#data_batch.points)]),
         dataflow:emit(Batch),
         State#state{mark = NewAt, at = NewAt, window = NewWindow,
            ts_list = KeepTsList, has_emitted = true};
      false ->
         State#state{window = NewWindow, ts_list = KeepTsList, at = NewAt}
   end.

evict(TimestampList, Window, At, Interval) ->
   {KeepTimestamps, Evict} = win_util:split(TimestampList, At - Interval),
%%   lager:info("evict: [~p] ~p~n keep [~p]: ~p",[length(Evict), [faxe_time:to_date(E) || E <- Evict],
%%      length(KeepTimestamps), [faxe_time:to_date(T) || T <- KeepTimestamps]]),
   {KeepTimestamps, win_util:sync_q(Window, Evict), length(Evict) > 0}.


check_emit(At, #state{mark = Mark, every = Every, fill_period = Fill}, EvictedOrEmitted) ->
   case At - Mark >= Every of
      true -> case Fill of
                 true -> EvictedOrEmitted;
                 false -> true
              end;
      false -> false
   end.

