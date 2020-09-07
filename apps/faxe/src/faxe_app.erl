%%%-------------------------------------------------------------------
%% @doc faxe public API
%% @end
%%%-------------------------------------------------------------------

-module(faxe_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1, prepare_stop/1]).

-define(PRIV_DIR, code:priv_dir(faxe)).
-define(COMPILE_OPTS, [{out_dir, ?PRIV_DIR ++ "/templates/"}]).
-define(APP, faxe).

%%====================================================================
%% API
%%====================================================================

start(_StartType, _StartArgs) ->
   print_banner(),
   %% Mnesia
   faxe_db:create(),
   %% COWBOY
   {ok, [Routes]} = file:consult(?PRIV_DIR ++ "/rest_routes.config"),
   Dispatch = cowboy_router:compile(Routes),
   HttpPort = application:get_env(faxe, http_api_port, 8081),
   {ok, _} = cowboy:start_clear(http_rest, [{port, HttpPort}],
      #{
         env =>  #{dispatch => Dispatch}, middlewares => [cowboy_router, cmw_headers, cowboy_handler]
      }
   ),
   %% start top supervisor
   Res = faxe_sup:start_link(),
   faxe_event_handlers:install(),
   print_started(HttpPort),
   Res.

%%--------------------------------------------------------------------
prepare_stop(State) ->
   lager:notice("Application faxe prepare to stop with state: ~p",[State]),
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

   io:format("* Configuration file: ~p - ~p~n",
      ["./etc/faxe.conf", CurrentDir ++ "/etc/faxe.conf"]),
   io:format("* REST Api listening on port: ~p ~n~n", [HttpPort]),
   io:format("** ~s ~s is running now!~n~n", [?APP, Vsn]).


%%====================================================================
%% Internal functions
%%====================================================================
