%%%-------------------------------------------------------------------
%% @doc faxe public API
%% @end
%%%-------------------------------------------------------------------

-module(faxe_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1, prepare_stop/1]).

-define(APP, faxe).
-define(PRIV_DIR, code:priv_dir(?APP)).

%%====================================================================
%% API
%%====================================================================

start(_StartType, _StartArgs) ->

   print_banner(),
   %% Mnesia
   faxe_db:db_init(),
   %% COWBOY
   {ok, [Routes]} = file:consult(filename:join(?PRIV_DIR, "rest_routes.config")),
   Dispatch = cowboy_router:compile(Routes),
   HttpPort = faxe_config:get(http_api_port, 8081),
   {StartFunction, SockOpts} =
      case faxe_config:get_http_tls() of
         true -> {start_tls, faxe_config:get_http_ssl_opts()};
         _ -> {start_clear, []}
      end,
   lager:notice("cowboy http(s) listener starting with: ~p",[{StartFunction, SockOpts}]),
   MaxConns = 200, Acceptors = 5,
   {ok, _} = cowboy:StartFunction(http,
      #{socket_opts => [{port, HttpPort}] ++ SockOpts,
      max_connections => MaxConns, num_acceptors => Acceptors},
      #{
         env =>  #{dispatch => Dispatch},
         middlewares => [cowboy_router, cmw_headers, cowboy_handler]
      }
   ),
   %% start top supervisor
   Res = faxe_sup:start_link(),
   faxe_event_handlers:install(),
   print_started(HttpPort),
   Res.

%%--------------------------------------------------------------------
prepare_stop(State) ->
   lager:notice("Application faxe prepares to stop with state: ~p",[State]),
   State.


stop(_State) ->
   ok.

%%--------------------------------------------------------------------
%% Print Banner
%%--------------------------------------------------------------------

print_banner() ->
   io:format("** Starting ~s on node ~s~n~n", [?APP, node()]).

print_started(HttpPort) ->
   {ok, _Description} = application:get_key(description),
   {ok, Vsn} = application:get_key(vsn),
   {ok, CurrentDir} = file:get_cwd(),

   io:format("~n* Configuration file: ~p - ~p~n",
      ["./etc/faxe.conf", CurrentDir ++ "/etc/faxe.conf"]),
   io:format("** REST Api listening on port: ~p ~n~n", [HttpPort]),
   io:format("*** ~s ~s is running now!~n~n", [?APP, Vsn]).


%%====================================================================
%% Internal functions
%%====================================================================
