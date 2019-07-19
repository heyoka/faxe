%% Date: 05.01.17 - 17:41
%% Ⓒ 2017 heyoka
%% @doc window which refers it's timing to the timestamp contained in the incoming data-items
%%
-module(esp_win_time).
-author("Alexander Minichmair").

-behaviour(df_component).

-include("faxe.hrl").

%% API
-export([init/3, process/3, handle_info/2, options/0]).

options() ->
   [{period, binary}, {every, binary}, {module, atom, c_noop}, {field, atom, <<"val">>}, {agg_param, any, #{}}].

init(NodeId, _Inputs, #{period := Period, every := Every, module := AggMod, field := F, agg_param := P} = Params) ->
   io:format("~p init:node ~p~n",[NodeId, Params]),
   Ev = faxe_time:duration_to_ms(Every),
   Per = faxe_time:duration_to_ms(Period),
   State = #esp_window{period = Per, every = Ev, agg_mod = AggMod, agg_fields = F},
%%   {ok, AggState} = AggMod:init([]),
   {ok, all, State#esp_window{agg = P}}.


process(_Inport, #data_point{ts = Ts} = Point, State=#esp_window{agg_fields = Field} ) ->

   Val = flowdata:field(Point, Field),
%%   lager:debug("~p got event pushed: ~p",[?MODULE, Point]),

   State1 = tick(State),
   NewStats = accumulate({Ts, Val, Point}, State1#esp_window.stats),


   {ok, State1#esp_window{stats = NewStats}}.

handle_info(Request, State) ->
   io:format("~p request: ~p~n", [State, Request]),
   {ok, State}.


%%%===================================================================
%%% Internal functions
%%%===================================================================

accumulate({Ts, Val, Event}, Stats = #esp_win_stats{events = {[], [], []}, count = C}) ->
   NewStats = Stats#esp_win_stats{events = {[Ts], [Val], [Event]}, count = C+1, at = Ts, mark = Ts},
   NewStats;
accumulate({Ts, Val, Event}, Stats = #esp_win_stats{events = {Tss, Vals, Evs}, count = C}) ->
   NewEvents = {Tss++[Ts], Vals++[Val], Evs++[Event]},
   NewStats = Stats#esp_win_stats{events = NewEvents, count = C+1, at = Ts},
   NewStats.

%% tick the window
tick(State = #esp_window{stats = #esp_win_stats{events = {[],[],[]} } }) ->
   State;
tick(State = #esp_window{agg_mod = _Module, agg = _Agg, every = Every, agg_fields = _Field,
   stats = #esp_win_stats{events = {_TimeStamps, _Values, _E} = Events, at = At, mark = Mark} = Stats, period = Interval}) ->

   % on a tick, we check for sliding out old events
   {{Tss, _Vs, Es}=Keep, _Evict} = evict(Events, At, Interval),
   NewAt = lists:last(Tss),
%%   lager:notice("on tick timespan is ~p | At-Mark is: ~p | AT : ~p",[NewAt-hd(Tss), NewAt-Mark, faxe_time:to_date(NewAt)]),
   case (NewAt - Mark) >= Every of
      true -> %{ok, Res, AggS} = Module:emit(Stats, NewAgg),
%%         Res = c_agg:call({Tss, Vs}, Module, Agg, Field),
         Batch = #data_batch{points = Es},
%%         lager:warning("~n when ~p ~p emitting: ~p",[NewAt-Mark, ?MODULE, {Batch, length(Batch#data_batch.points)}]),
         dataflow:emit(Batch),
         State#esp_window{stats = Stats#esp_win_stats{events = Keep, mark = NewAt, at = NewAt}};
      false ->
         State#esp_window{stats = Stats#esp_win_stats{events = Keep, at = NewAt}}
   end.

%%%% filter list and evict old entries, with early abandoning
-spec evict(window_events(), non_neg_integer(), non_neg_integer()) -> {window_events(), list()}.
evict({Ts, Vals, Events}, At, Interval) ->
   {Keep, Evict} = win_util:split(Ts, At - Interval),
%%   lager:info("Evicted: ~p",[Evict]),
   { {Keep,  win_util:sync(Vals, Evict), win_util:sync(Events, Evict)},  Evict}
.

