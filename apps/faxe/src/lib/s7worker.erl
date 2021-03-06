%%%-------------------------------------------------------------------
%%% @author heyoka
%%% @copyright (C) 2020, <COMPANY>
%%% @doc
%%% @end
%%%-------------------------------------------------------------------
-module(s7worker).

-behaviour(gen_server).

-export([start_link/1, read/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
  code_change/3]).



-define(RECON_MIN_INTERVAL, 10).
-define(RECON_MAX_INTERVAL, 400).
-define(RECON_MAX_RETRIES, infinity).


-record(state, {
  ip,
  port,
  client,
  slot,
  rack,
  reconnector,
  owner
}).

%%%===================================================================
%%% Spawning and gen_server implementation
%%%===================================================================

read(Pid, Vars) ->
  gen_server:call(Pid, {read, Vars}).

start_link(#{} = Opts) ->
  gen_server:start_link(?MODULE, Opts#{owner => self()}, []).

init(#{ip := Ip, port := Port, slot := Slot, rack := Rack, owner := Owner}) ->
  Reconnector = faxe_backoff:new(
    {?RECON_MIN_INTERVAL, ?RECON_MAX_INTERVAL, ?RECON_MAX_RETRIES}),
  erlang:send_after(0, self(), connect),
  {ok, #state{
    ip = Ip,
    port = Port,
    slot = Slot,
    rack = Rack,
    reconnector = Reconnector,
    owner = Owner
  }}.

handle_call({read, Opts}, _From, State = #state{client = Client, ip = _Ip}) ->
%%  lager:notice("READ::: ~p Q-Length: ~p || ~p",[
%%    self(), erlang:process_info(self(), message_queue_len), erlang:process_info(Client, message_queue_len)]),
  Res = (catch snapclient:read_multi_vars(Client, Opts)),
%%  lager:notice("read RESULT: ~p",[Res]),
  {reply, Res, State};
handle_call(_Request, _From, State = #state{}) ->
  {reply, ok, State}.

handle_cast(_Request, State = #state{}) ->
  {noreply, State}.

%% client process is down,
%% we match the Object field from the DOWN message against the current client pid
handle_info({snap7_connected, Client}, State = #state{client = Client, owner = Owner}) ->
%%  lager:notice("snap7_connected: ~p", [Client]),
  Owner ! {up, self()},
  {noreply, State#state{client = Client}};
handle_info({'DOWN', _MonitorRef, _Type, Client, Info}, State=#state{client = Client, owner = Owner}) ->
  lager:warning("Snap7 Client process is DOWN with : ~p ! ", [Info]),
  Owner ! {down, self()},
  try_reconnect(State#state{client = undefined});
%% old DOWN message from already restarted client process
handle_info({'DOWN', _MonitorRef, _Type, _Object, _Info}, State) ->
  {noreply, State};
handle_info(connect,
    State=#state{ip = Ip, port = Port, rack = Rack, slot = Slot}) ->
%%  lager:info("[~p] do_reconnect, ~p", [?MODULE, {Ip, Port}]),
  case connect(Ip, Rack, Slot) of
    {ok, Client} ->
      {noreply, State#state{client = Client}};
    {error, Error} ->
      lager:error("[~p] Error connecting to PLC ~p: ~p",[?MODULE, {Ip, Port},Error]),
      try_reconnect(State)
  end;
handle_info(_E, S) ->
  lager:warning("unhandled Req: ~p" ,[_E]),
  {noreply, S}.

terminate(_Reason, _State = #state{client = Client}) ->
  catch (snapclient:disconnect(Client)),
  catch (snapclient:stop(Client)).

code_change(_OldVsn, State = #state{}, _Extra) ->
  {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
try_reconnect(State=#state{reconnector = Reconnector}) ->
  case faxe_backoff:execute(Reconnector, connect) of
    {ok, Reconnector1} ->
      {noreply, State#state{reconnector = Reconnector1}};
    {stop, Error} -> logger:error("[Client: ~p] PLC reconnect error: ~p!",[?MODULE, Error]),
      {stop, {shutdown, Error}, State}
  end.

connect(Ip, Rack, Slot) ->
  case catch do_connect(Ip, Rack, Slot) of
    Client when is_pid(Client) -> {ok, Client};
    Err -> {error, Err}
  end.

do_connect(Ip, Rack, Slot) ->
  {ok, Client} = snapclient:start_connect(#{ip => Ip, rack => Rack, slot => Slot}),
  erlang:monitor(process, Client),
  Client.
