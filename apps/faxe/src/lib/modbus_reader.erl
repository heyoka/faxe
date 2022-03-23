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
  requests = [],
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

handle_info({'DOWN', _MonitorRef, process, _Object, _Info}, State=#state{parent = Parent}) ->
  Parent ! {modbus, disconnected},
  NewState = connect(State),
  {noreply, NewState};
handle_info({modbus, _From, connected}, S = #state{parent = Parent, last_request = LR}) ->
  Parent ! {modbus, self(), connected},
  case LR of
    {Reference, ReadReq} ->
      erlang:send_after(0, self(), {read, Reference, ReadReq});
    undefined -> ok
  end,
  NewState = S#state{connected = true, last_request = undefined},
  {noreply, NewState};
%% if disconnected, we just wait for a connected message and stop polling in the mean time
handle_info({modbus, _From, disconnected}, State=#state{parent = Parent}) ->
  Parent ! {modbus, self(), disconnected},
  {noreply, State#state{connected = false, requests = []}};
handle_info({modbusdata, {error, TId, What}}, State=#state{parent = Parent}) ->
  lager:warning("~~~~~~~~~~~~~~~~~~~~~ modbusdata error for TId: ~p :~p",[TId, What]),
  NewState =
  case lists:keytake(TId, 1, State#state.requests) of
    false ->
      lager:warning("transaction-reference ~p not found when error state", [TId]),
      State;
    {value, {TId, {Ref, #{aliases := _Aliases}}}, Reqs} ->
      Parent ! {modbus_data, self(), Ref, {error, What}},
      State#state{requests = Reqs}
  end,
  {noreply, NewState};
handle_info({modbusdata, {ok, TId, Data}}, State=#state{parent = Parent}) ->
  NewState =
  case lists:keytake(TId, 1, State#state.requests) of
    false ->
      logger:warning("transaction-reference ~p not found when ok response", [TId]),
      State;
    {value, {TId, {Ref, _Opts = #{aliases := Aliases}}}, Reqs} ->
      Parent ! {modbus_data, self(), Ref, {ok, lists:zip(Aliases, Data)}},
      State#state{requests = Reqs}
  end,
  {noreply, NewState};
handle_info({read, Ref, ReadReq}, State = #state{requests = Reqs}) ->
  Res = read(ReadReq, State),
  NewState =
  case Res of
    {ok, TId} ->
      NewRequests = [{TId, {Ref, ReadReq}}|Reqs],
      State#state{requests = NewRequests};
    {error, disconnected} -> %% save the Req for later
      State#state{last_request = {Ref, ReadReq}};
    {error, What} ->
      lager:warning("error reading from modbus: ~p",[What]),
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

read(#{function := Fun, start := Start, amount := Amount, opts := Opts} = Req,
    _State = #state{client = Client}) ->
  Res = modbus:Fun(Client, Start, Amount, Opts),
  case Res of
    {error, disconnected} ->
      lager:notice("retry reading from modbus: disconnected (~p)",[Req]),
      %% connect(State),
      {error, disconnected};
    {error, _Reason} ->
      lager:error("error reading from modbus: ~p (~p)",[_Reason, Req]),
      {error, _Reason};
    {ok, TId} ->
      {ok, TId}
  end.
