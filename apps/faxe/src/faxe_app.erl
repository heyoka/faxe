%%%-------------------------------------------------------------------
%% @doc faxe public API
%% @end
%%%-------------------------------------------------------------------

-module(faxe_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

-define(PRIV_DIR, code:priv_dir(faxe)).
-define(COMPILE_OPTS, [{out_dir, ?PRIV_DIR ++ "/templates/"}]).

%%====================================================================
%% API
%%====================================================================

start(_StartType, _StartArgs) ->
   %% Mnesia
   faxe_db:create(),
   %% COWBOY
   {ok, [Routes]} = file:consult(?PRIV_DIR ++ "/rest_routes.config"),
   Dispatch = cowboy_router:compile(Routes),
   HttpPort = application:get_env(faxe, http_api_port, 8081),
   {ok, _} = cowboy:start_clear(http_rest, [{port, HttpPort}],
      #{env =>  #{dispatch => Dispatch}}
   ),
   %% start top supervisor
   faxe_sup:start_link().

%%--------------------------------------------------------------------
stop(_State) ->
   ok.

%%====================================================================
%% Internal functions
%%====================================================================
