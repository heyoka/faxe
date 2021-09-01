%%%-------------------------------------------------------------------
%%% @author heyoka
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%% manages cowboy instances for http_listen nodes
%%% @end
%%%-------------------------------------------------------------------
-module(http_manager).

-behaviour(gen_server).

-export([start_link/0, reg/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
  code_change/3]).

-define(SERVER, ?MODULE).

-define(NUM_ACCEPTORS, 2).
-define(MAX_CONNECTIONS, 15).

-record(state, {
  cowboys = [] :: list()
}).

%%%===================================================================
%%% Spawning and gen_server implementation
%%%===================================================================
reg(Opts) ->
  gen_server:call(?SERVER, {reg, Opts}).

start_link() ->
  gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

init([]) ->
  {ok, #state{}}.

handle_call({reg, #{port := Port} = Opts} , From, State = #state{cowboys = Cowboys}) ->
  case available(Port, Cowboys) of
    true -> {reply, ok, start_cowboy(Opts, From, State)};
    false -> lager:warning("[Cowboy-Manager] Port ~p already in use", [Port]),
      {reply, {error, port_in_use}, State}
  end;
handle_call(_Request, _From, State = #state{}) ->
  {reply, ok, State}.

handle_cast(_Request, State = #state{}) ->
  {noreply, State}.

handle_info({'DOWN', _MonitorRef, _Type, Pid, _Info}, State = #state{cowboys = Clients}) ->
  connection_registry:disconnected(),
  lager:warning("client is down"),
  case proplists:get_value(Pid, Clients) of
    undefined -> ok;
    InstanceName -> cowboy:stop_listener(InstanceName)
  end,
  {noreply, State#state{cowboys = proplists:delete(Pid, Clients)}};
handle_info(_Info, State = #state{}) ->
  {noreply, State}.

terminate(_Reason, _State = #state{cowboys = Clients}) ->
  [cowboy:stop_listener(InstanceName) || {_ClientPid, InstanceName} <- Clients],
  ok.

code_change(_OldVsn, State = #state{}, _Extra) ->
  {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
start_cowboy(#{path := Route, port := Port, tls := Tls, content_type := ContentType, user := User, pass := Pass},
    {From, _Ref}, State = #state{cowboys = Clients}) ->
  CowboyRef = {http_listen, Port},
  Routes = [
    {'_', [
      {Route, http_listen_handler, [{client, From}, {content_type, ContentType}, {user, User}, {pass, Pass}]}
      ]
    }],

  Dispatch = cowboy_router:compile(Routes),
  {StartFunction, SockOpts} =
    case Tls of
      true -> {start_tls, faxe_config:get_http_ssl_opts()};
      _ -> {start_clear, []}
    end,
  lager:info("cowboy http(s) listener starting with: ~p on Port: ~p", [{StartFunction, SockOpts}, Port]),
  {ok, _} = cowboy:StartFunction(CowboyRef,
    #{socket_opts => [{port, Port}] ++ SockOpts,
      max_connections => ?MAX_CONNECTIONS,
      num_acceptors => ?NUM_ACCEPTORS},
    #{env => #{dispatch => Dispatch},
      middlewares => [cowboy_router, cmw_headers, cowboy_handler]
    }
  ),
  erlang:monitor(process, From),
  State#state{cowboys = [{From, CowboyRef} | Clients]}.

available(DesiredPort, Cowboys) ->
  lists:all(fun({_Pid, {_, Port}}) -> DesiredPort /= Port end, Cowboys).