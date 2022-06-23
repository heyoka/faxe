%%%-------------------------------------------------------------------
%%% @author heyoka
%%% @copyright (C) 2022, <COMPANY>
%%% @doc
%%% @end
%%%-------------------------------------------------------------------
-module(faxe_event_guard).

-behaviour(gen_server).

-export([start_link/3]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
  code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {
  event,
  module,
  config
}).

%%%===================================================================
%%% Spawning and gen_server implementation
%%%===================================================================

start_link(Event, Module, Config) ->
  gen_server:start_link(?MODULE, [Event, Module, Config], []).

init([Event, Module, Config]) ->
  install_handler(Event, Module, Config),
  {ok, #state{event=Event, module=Module, config=Config}}.

handle_call(_Request, _From, State = #state{}) ->
  {reply, ok, State}.

handle_cast(_Request, State = #state{}) ->
  {noreply, State}.

handle_info({gen_event_EXIT, Module, normal}, #state{module=Module} = State) ->
  {stop, normal, State};
handle_info({gen_event_EXIT, Module, shutdown}, #state{module=Module} = State) ->
  {stop, normal, State};
handle_info({gen_event_EXIT, Module, Reason},
    #state{event=Event, module=Module, config=Config} = State) ->
  lager:notice("event-handler ~p exit with reason: ~p",[Module, Reason]),
  install_handler(Event, Module, Config),
  {noreply, State};
handle_info(_Info, State = #state{}) ->
  {noreply, State}.

terminate(_Reason, _State = #state{}) ->
  ok.

code_change(_OldVsn, State = #state{}, _Extra) ->
  {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
install_handler(Event, Module, Config) ->
  ok = gen_event:add_sup_handler(Event, Module, Config).