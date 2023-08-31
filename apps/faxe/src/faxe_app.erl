%%%-------------------------------------------------------------------
%% @doc faxe public API
%% @end
%%%-------------------------------------------------------------------

-module(faxe_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1, prep_stop/1]).

-define(APP, faxe).
-define(PRIV_DIR, code:priv_dir(?APP)).

%%====================================================================
%% API
%%====================================================================

start(_StartType, _StartArgs) ->
%%   logger:remove_handler(default),
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
         middlewares => [cowboy_router, cmw_headers, cowboy_handler],
         stream_handlers => [cowboy_compress_h, cowboy_stream_h] % ?
      }
   ),
   %% start top supervisor
   Res = faxe_sup:start_link(),
   print_started(HttpPort),
   Res.

%%--------------------------------------------------------------------
prep_stop(State) ->
   NState = faxe_time:now(),
   faxe:stop_all(),
   lager:info("Application faxe prepares to stop with state: ~p",[State]),
   NState.


stop(_PrepTime) ->
   lager:notice("Faxe on ~p stopping", [faxe_util:device_name()]),
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
   FlowStatePersistenceActive =
   case faxe_config:get_sub(flow_state_persistence, enable, false) of
      true -> <<"">>;
      false -> <<"NOT ">>
   end,

   io:format("~n* Configuration file: ~p - ~p~n",
      ["./etc/faxe.conf", CurrentDir ++ "/etc/faxe.conf"]),
   io:format("** REST Api listening on port: ~p ~n~n", [HttpPort]),
   io:format("** Flow state persistence is ~sactive~n~n", [FlowStatePersistenceActive]),
   io:format("*** ~s ~s is running now!~n~n", [?APP, Vsn]).


%%====================================================================
%% Internal functions
%%====================================================================
