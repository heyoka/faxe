%%%-------------------------------------------------------------------
%%% @author heyoka
%%% @copyright (C) 2024, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 18. Mar 2024 11:00 AM
%%%-------------------------------------------------------------------
-module(dfs_debug_handler).
-author("heyoka").

-behaviour(gen_event).

-export([code_change/3, handle_call/2, handle_event/2,
  handle_info/2, init/1, terminate/2]).

-record(state, {flow_name}).

init([FlowName]) ->
  lager:md([{flow, FlowName}]),
  {ok, #state{flow_name = FlowName}}.

handle_event(#{expression := Expr0, result := Result0}, State) ->
  Expr = clean(Expr0),
  lager:notice("dfs_debug: Expression: ~s || Result: ~p~n", [Expr, Result0]),
  {ok, State};
handle_event(Event, State) ->
  lager:notice("unexpected event  ~p: ~p~n", [?MODULE, Event]),
  {ok, State}.

handle_call(Request, State) ->
  lager:notice("unexpected call to ~p: ~p~n", [?MODULE, Request]),
  {ok, State, State}.

code_change(_OldVsn, State, _Extra) -> {ok, State}.

handle_info(_Info, State) -> {noreply, State}.

terminate(_Args, _State) -> ok.

clean(String) ->
  %% remove the modules
  string:replace(string:replace(String, "faxe_lambda_lib:", "", all), "dfs_std_lib:", "", all).