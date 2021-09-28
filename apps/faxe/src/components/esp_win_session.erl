%% Date: 26.09.21 - 19:41
%% Ⓒ 2021 heyoka
%% @doc
%% A session window aggregates records into a session, which represents a period of activity separated by a specified gap of inactivity.
%% Any data_points with timestamps that occur within the inactivity gap of existing sessions will be added to this session.
%% If a data_points's timestamp occurs outside of the session gap, a new session is created.
%% A new session window starts if the last record that arrived is further back in time than the specified inactivity gap.
%%
%% window which refers it's timing to the timestamp contained in the incoming data-items just like the win_time node does
%% but the window boundaries will be determined by an inactivity gap parameter (session_timeout)
%% Session window durations vary !
%% @todo
%%
-module(esp_win_session).
-author("Alexander Minichmair").

-behaviour(df_component).

-include("faxe.hrl").

%% API
-export([init/3, process/3, handle_info/2, options/0, wants/0, emits/0]).

-record(state, {
   window :: queue:queue(),
   last_ts = undefined,
   inactivity_gap
}).

options() ->
   [
      {session_timeout, duration, <<"30s">>}
   ].

wants() -> point.
emits() -> batch.

init(_NodeId, _Inputs, #{session_timeout := STimeout}) ->
   GapSize = faxe_time:duration_to_ms(STimeout),
   State = #state{window = queue:new(), inactivity_gap = GapSize},
   {ok, all, State}.

process(_Inport, Point = #data_point{ts = Ts}, State = #state{window = Win, last_ts = undefined}) ->
   {ok, State#state{last_ts = Ts, window = queue:in(Point, Win)}};
process(_Inport, Point = #data_point{ts = Ts}, State = #state{window = Win, last_ts = LastTs, inactivity_gap = Gap})
      when (Ts-LastTs) =< Gap ->
   {ok, State#state{last_ts = Ts, window = queue:in(Point, Win)}};
process(_Inport, Point = #data_point{ts = Ts}, State = #state{window = Win}) ->
   Batch = #data_batch{points = queue:to_list(Win)},
   WinNew = queue:new(),
   {emit, Batch, State#state{last_ts = Ts, window = queue:in(Point, WinNew)}}.


handle_info(_Request, State) ->
   {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

