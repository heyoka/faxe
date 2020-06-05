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
      #{
         env =>  #{dispatch => Dispatch}, middlewares => [cowboy_router, cmw_headers, cowboy_handler]
      }
   ),
   %% start top supervisor
   Res = faxe_sup:start_link(),
   install_metrics_handler(),
   Res.

%%--------------------------------------------------------------------
stop(_State) ->
   ok.

%%====================================================================
%% Internal functions
%%====================================================================
install_metrics_handler() ->
   %% standard handler
   dataflow:add_metrics_handler(),
   Handlers = proplists:get_value(handler, faxe_config:get(metrics, [{handler, []}])),
   lager:notice("HAndlers: ~p",[Handlers]),
   F = fun({HandlerType, Opts}) ->
      case HandlerType of
         mqtt -> dataflow:add_metrics_handler(metrics_handler_mqtt, Opts);
         amqp -> dataflow:add_metrics_handler(metrics_handler_amqp, Opts);
         _ -> ok
      end
      end,
   lists:foreach(F, Handlers).

