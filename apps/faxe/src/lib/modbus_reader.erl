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
  parent
}).

%%%===================================================================
%%% Spawning and gen_server implementation
%%%===================================================================

start_link(Ip, Port, DeviceAddress) ->
  gen_server:start_link(?MODULE, [Ip, Port, DeviceAddress, self()], []).

init([Ip, Port, DeviceAddress, Parent1]) ->
  State = #state{ip = Ip, port = Port, device_address = DeviceAddress, parent = Parent1},
  NewState = connect(State),
  lager:info("modbus reader starting ..."),
  {ok, NewState}.

handle_call(_Request, _From, State = #state{}) ->
  {reply, ok, State}.

handle_cast(_Request, State = #state{}) ->
  {noreply, State}.

handle_info({'DOWN', _MonitorRef, process, _Object, Info}, State=#state{parent = Parent}) ->
  Parent ! {modbus, disconnected},
  lager:warning("Modbus process is DOWN with : ~p !", [Info]),
  NewState = connect(State),
  {noreply, NewState};
handle_info({modbus, _From, connected}, S = #state{parent = Parent}) ->
  Parent ! {modbus, connected},
  lager:notice("~p modbus connected !",[?MODULE]),
%%  connection_registry:connected(),
%%   lager:notice("Modbus is connected, lets start polling ..."),
%%  Timer = faxe_time:init_timer(S#state.align, S#state.interval, poll),
  NewState = S#state{connected = true},
  {noreply, NewState};
%% if disconnected, we just wait for a connected message and stop polling in the mean time
handle_info({modbus, _From, disconnected}, State=#state{parent = Parent}) ->
  lager:notice("~p modbus disconnected !", [?MODULE]),
  Parent ! {modbus, disconnected},
  {noreply, State#state{connected = false}};
handle_info({read, Fun, Start, Count, Opts} = Req, State = #state{parent = P}) ->
  Res = read({{Fun, Start, Count}, Opts}, State),
  lager:notice("Result from modbus: ~p // ~p",[Res, Req]),
  P ! {modbus_data, self(), Res},
  {noreply, State};
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

read({{Fun, Start, Count}=F, Opts}, State = #state{client = Client}) ->
  Res = modbus:Fun(Client, Start, Count, Opts),
  case Res of
    {error, disconnected} ->
      connect(State),
      {error, stop};
    {error, _Reason} ->
      lager:error("error reading from modbus: ~p (~p)",[_Reason, F]),
      {error, _Reason};
    Data ->
      FData =
        case Data of
          [D] -> D;
          _ -> Data
        end,
      {ok, FData}
  end.