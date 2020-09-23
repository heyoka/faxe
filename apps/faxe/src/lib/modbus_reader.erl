%%%-------------------------------------------------------------------
%%% @author heyoka
%%% @copyright (C) 2020, <COMPANY>
%%% @doc
%%% @end
%%%-------------------------------------------------------------------
-module(modbus_reader).

-behaviour(gen_server).

-export([start_link/3]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
  code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {
  ip,
  port,
  device_address,
  client,
  connected,
  parent,
  last_request
}).

%%%===================================================================
%%% Spawning and gen_server implementation
%%%===================================================================

start_link(Ip, Port, DeviceAddress) ->
  gen_server:start_link(?MODULE, [Ip, Port, DeviceAddress, self()], []).

init([Ip, Port, DeviceAddress, Parent1]) ->
  State = #state{ip = Ip, port = Port, device_address = DeviceAddress, parent = Parent1},
  NewState = connect(State),
  {ok, NewState}.

handle_call(_Request, _From, State = #state{}) ->
  {reply, ok, State}.

handle_cast(_Request, State = #state{}) ->
  {noreply, State}.

handle_info({'DOWN', _MonitorRef, process, _Object, Info}, State=#state{parent = Parent}) ->
  Parent ! {modbus, disconnected},
%%  lager:warning("Modbus process is DOWN with : ~p !", [Info]),
  NewState = connect(State),
  {noreply, NewState};
handle_info({modbus, _From, connected}, S = #state{parent = Parent, last_request = LR}) ->
  Parent ! {modbus, self(), connected},
%%  lager:debug("~p modbus connected !",[?MODULE]),
  case LR of
    {Reference, ReadReq} ->
      Res = read(ReadReq, S),
%%      lager:notice("we have a pending request, do it: ~p",[Res]),
      Parent ! {modbus_data, self(), Reference, Res};
    undefined -> ok
  end,
  NewState = S#state{connected = true, last_request = undefined},
  {noreply, NewState};
%% if disconnected, we just wait for a connected message and stop polling in the mean time
handle_info({modbus, _From, disconnected}, State=#state{parent = Parent}) ->
%%  lager:debug("~p modbus disconnected !", [?MODULE]),
  Parent ! {modbus, self(), disconnected},
  {noreply, State#state{connected = false}};
handle_info({read, Ref, ReadReq}, State = #state{parent = P}) ->
  Res = read(ReadReq, State),
  NewState =
  case Res of
    {error, disconnected} -> %% save the Req for later
      State#state{last_request = {Ref, ReadReq}};
    _D ->
      P ! {modbus_data, self(), Ref, Res},
      State
  end,
  {noreply, NewState};
handle_info(_E, S) ->
  {noreply, S#state{}}.

terminate(_Reason, _State = #state{client = Modbus}) ->
  catch modbus:disconnect(Modbus).

code_change(_OldVsn, State = #state{}, _Extra) ->
  {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
connect(State = #state{}) ->
  {ok, Modbus} = modbus:connect(State#state.ip, State#state.port, State#state.device_address),
  erlang:monitor(process, Modbus),
  State#state{client = Modbus, connected = false}.

read(#{function := Fun, start := Start, amount := Amount, opts := Opts, aliases := Aliases} = Req,
    _State = #state{client = Client}) ->
  Res = modbus:Fun(Client, Start, Amount, Opts),
  case Res of
    {error, disconnected} ->
      lager:error("retry reading from modbus: disconnected (~p)",[Req]),
      %% connect(State),
      {error, disconnected};
    {error, _Reason} ->
      lager:error("error reading from modbus: ~p (~p)",[_Reason, Req]),
      {error, _Reason};
    Data ->
%%      lager:info("got data: ~p",[Data]),
      {ok, lists:zip(Aliases, Data)}
  end.
