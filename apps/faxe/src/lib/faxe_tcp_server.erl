-module(faxe_tcp_server).

-behaviour(gen_statem).
-behaviour(ranch_protocol).

%% API.
-export([start_link/4]).

%% gen_statem.
-export([callback_mode/0]).
-export([init/1]).
-export([connected/3]).
-export([terminate/3]).
-export([code_change/4]).

-define(TIMEOUT, 5000).

-record(state, {socket, transport, parent}).

%% API.

start_link(Ref, _Socket, Transport, Opts) ->
  {ok, proc_lib:spawn_link(?MODULE, init, [{Ref, Transport, Opts}])}.

%% gen_statem.

callback_mode() ->
  state_functions.

init({Ref, Transport, #{parent := Parent, tcp_opts := Opts}}) ->
  {ok, Socket} = ranch:handshake(Ref),
  ok = Transport:setopts(Socket, Opts),
  Parent ! {tcp_server_up, self()},
  gen_statem:enter_loop(?MODULE, [], connected,
    #state{socket = Socket, transport = Transport, parent = Parent}).

connected(info, {data, Data}, #state{socket=Socket, transport=Transport}) ->
  Transport:send(Socket, Data),
  keep_state_and_data;
connected(info, {tcp_closed, _Socket}, _StateData) ->
  {stop, normal};
connected(info, {tcp_error, _, Reason}, _StateData) ->
  {stop, Reason};
connected({call, From}, _Request, _StateData) ->
  gen_statem:reply(From, ok),
  keep_state_and_data;
connected(cast, _Msg, _StateData) ->
  keep_state_and_data;
connected(timeout, _Msg, _StateData) ->
  {stop, normal};
connected(_EventType, _Msg, _StateData) ->
  io:format("~ngot: ~p~n",[_Msg]),
  {stop, normal}.

terminate(Reason, StateName, StateData=#state{socket=Socket, transport=Transport})
  when Socket=/=undefined andalso Transport=/=undefined ->

  catch Transport:close(Socket),
  terminate(Reason, StateName,
    StateData#state{socket=undefined, transport=undefined});
terminate(_Reason, _StateName, _StateData) ->
  ok.

code_change(_OldVsn, StateName, StateData, _Extra) ->
  {ok, StateName, StateData}.

