%% Date: 05.01.17 - 17:41
%% Ⓒ 2017 heyoka
%% @doc window which refers it's timing to the timestamp contained in the incoming data-items
%% rewrite with queue module instead of lists
%% @todo setup timeout where points get evicted even though there a timestamps missing ?
%%
-module(esp_win_time_q).
-author("Alexander Minichmair").

-behaviour(df_component).

-include("faxe.hrl").

%% API
-export([init/3, process/3, handle_info/2, options/0]).

-record(state, {
   every,
   period,
   window,
   ts_list = [], %% list of inserted Timestamps
   at,
   mark,
   fill_period,
   has_emitted = false %% false as long as the window never has emitted values
}).

options() ->
   [{period, binary}, {every, binary}, {fill_period, is_set}].

init(NodeId, _Inputs, #{period := Period, every := Every, fill_period := Fill} = Params) ->
   io:format("~p init:node ~p~n",[NodeId, Params]),
   Ev = faxe_time:duration_to_ms(Every),
   Per = faxe_time:duration_to_ms(Period),
   %% fill_period does not make sense, if every is less than period
   DoFill = (Fill == true) andalso (Per > Ev),
   State = #state{period = Per, every = Ev, fill_period = DoFill, window = queue:new()},
   {ok, all, State}.


process(_Inport, #data_point{} = Point, State=#state{} ) ->
   State1 = tick(State),
   NewState = accumulate(Point, State1),
   {ok, NewState}.

handle_info(Request, State) ->
   io:format("~p request: ~p~n", [State, Request]),
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
tick(State = #state{mark = Mark, at = At, window = Win, period = Interval,
      every = Every, ts_list = TsList, fill_period = Fill}) ->

   {KeepTsList, NewWindow, HasEvicted} = evict(TsList, Win, At, Interval),
   NewAt = lists:last(KeepTsList),
   case check_emit(NewAt, Mark, Every, Fill, (HasEvicted orelse State#state.has_emitted)) of
      true ->
         Batch = #data_batch{points = queue:to_list(NewWindow)},
         lager:warning("~n when ~p period: ~p emitting: ~p",[NewAt-Mark, Interval, length(Batch#data_batch.points)]),
         dataflow:emit(Batch),
         State#state{mark = NewAt, at = NewAt, window = NewWindow, ts_list = KeepTsList, has_emitted = true};
      false ->
         State#state{window = NewWindow, ts_list = KeepTsList, at = NewAt}
   end.

evict(TimestampList, Window, At, Interval) ->
   {KeepTimestamps, Evict} = win_util:split(TimestampList, At - Interval),
   lager:info("evict: [~p] ~p~n keep [~p]: ~p",[length(Evict), [faxe_time:to_date(E) || E <- Evict],
      length(KeepTimestamps), [faxe_time:to_date(T) || T <- KeepTimestamps]]),
   {KeepTimestamps, win_util:sync_q(Window, Evict), length(Evict) > 0}.


check_emit(At, Mark, Every, Fill, EvictedOrEmitted) ->
   case At - Mark >= Every of
      true -> case Fill of
                 true -> EvictedOrEmitted;
                 false -> true
              end;
      false -> false
   end.

