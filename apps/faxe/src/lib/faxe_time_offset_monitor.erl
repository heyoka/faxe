%%%-------------------------------------------------------------------
%%% @author heyoka
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%% @end
%%%-------------------------------------------------------------------
-module(faxe_time_offset_monitor).

-behaviour(gen_server).

-export([start_link/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
  code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {current_offset = 0}).

%%%===================================================================
%%% Spawning and gen_server implementation
%%%===================================================================

start_link() ->
  gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

init([]) ->
  erlang:monitor(time_offset, clock_service),
  {ok, #state{current_offset = erlang:time_offset(milli_seconds)}}.

handle_call(_Request, _From, State = #state{}) ->
  {reply, ok, State}.

handle_cast(_Request, State = #state{}) ->
  {noreply, State}.

handle_info({'CHANGE', _MonitorReference, time_offset, clock_service, NewTimeOffset},
    State = #state{current_offset = Off}) ->
  OffsetMs = erlang:convert_time_unit(NewTimeOffset, native, milli_seconds),
  lager:warning("TIME_OFFSET changed by ~pms to: ~p", [Off-OffsetMs, OffsetMs]),
  {noreply, State#state{current_offset = OffsetMs}};
handle_info(_Info, State = #state{}) ->
  lager:info("[~p]got info: ~p",[?MODULE, _Info]),
  {noreply, State}.

terminate(_Reason, _State = #state{}) ->
  ok.

code_change(_OldVsn, State = #state{}, _Extra) ->
  {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
