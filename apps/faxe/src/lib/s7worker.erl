%%%-------------------------------------------------------------------
%%% @author heyoka
%%% @copyright (C) 2020, <COMPANY>
%%% @doc
%%% @end
%%%-------------------------------------------------------------------
-module(s7worker).

-behaviour(gen_server).

-export([start_link/1, read/2, start_monitor/1, get_pdu_size/1]).
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

get_pdu_size(Pid) ->
  gen_server:call(Pid, {get_pdu_size}).

start_link(#{} = Opts) ->
  gen_server:start_link(?MODULE, Opts#{owner => self()}, []).

start_monitor(#{} = Opts) ->
  lager:notice("Opts for s7worker: ~p",[Opts]),
  case gen_server:start(?MODULE, Opts#{owner => self()}, []) of
    {ok, Pid} -> erlang:monitor(process, Pid), {ok, Pid};
    Other -> Other
  end.

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
  Res = (catch snapclient:read_multi_vars(Client, Opts)),
  {reply, Res, State};
handle_call({get_pdu_size}, _From, State = #state{client = Client}) ->
  Res = (catch snapclient:get_pdu_length(Client)),
  {reply, Res, State};
handle_call(_Request, _From, State = #state{}) ->
  {reply, ok, State}.

handle_cast(_Request, State = #state{}) ->
  {noreply, State}.

%% client process is down,
%% we match the Object field from the DOWN message against the current client pid
handle_info({snap7_connected, Client}, State = #state{client = Client, owner = Owner}) ->
  Owner ! {s7_connected, self()},
  {noreply, State#state{client = Client}};
handle_info({'DOWN', _MonitorRef, _Type, Client, Info}, State=#state{client = Client, owner = Owner}) ->
  lager:info("Snap7 Client process is DOWN with : ~p ! ", [Info]),
  Owner ! {s7_disconnected, self()},
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
