%%%-------------------------------------------------------------------
%%% @author heyoka
%%% @copyright (C) 2020, <COMPANY>
%%% @doc
%%% @end
%%%-------------------------------------------------------------------
-module(rest_audit_server).

-behaviour(gen_server).

-export([start_link/0, audit/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
  code_change/3]).

-define(SERVER, ?MODULE).

-record(rest_audit_server_state, {}).

%%%===================================================================
%%% Spawning and gen_server implementation
%%%===================================================================
audit(User, Req) ->
  ?SERVER ! {audit, User, Req}.

start_link() ->
  gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

init([]) ->
  {ok, #rest_audit_server_state{}}.

handle_call(_Request, _From, State = #rest_audit_server_state{}) ->
  {reply, ok, State}.

handle_cast(_Request, State = #rest_audit_server_state{}) ->
  {noreply, State}.

handle_info({audit, User, _CowboyReq = #{path := Path, bindings := Bindings}}, State = #rest_audit_server_state{}) ->
  lager:notice("[AUDIT-LOG] Api-User: ~s || Path: ~s || Bindings: ~p", [User, Path, Bindings]),
  {noreply, State}.

terminate(_Reason, _State = #rest_audit_server_state{}) ->
  ok.

code_change(_OldVsn, State = #rest_audit_server_state{}, _Extra) ->
  {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
