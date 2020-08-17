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
   install_metrics_handler(),
   install_conn_status_handler(),
   install_debug_handler(),
   print_vsn(),
   Res.

%%--------------------------------------------------------------------
stop(_State) ->
   ok.


%%--------------------------------------------------------------------
%% Print Banner
%%--------------------------------------------------------------------

print_banner() ->
   io:format("Starting ~s on node ~s~n", [?APP, node()]).

print_vsn() ->
%%   {ok, Descr} = application:get_key(description),
   {ok, Vsn} = application:get_key(vsn),
   io:format("~s ~s is running now!~n", [?APP, Vsn]).


%%====================================================================
%% Internal functions
%%====================================================================
install_metrics_handler() ->
   %% standard handler
%%   dataflow:add_metrics_handler(),
   Handlers = proplists:get_value(handler, faxe_config:get(metrics, [{handler, []}] )),
   lager:notice("Metric HAndlers: ~p",[Handlers]),
   F = fun({HandlerType, Opts}) ->
      case HandlerType of
         mqtt -> dataflow:add_metrics_handler(metrics_handler_mqtt, event_handler_mqtt, Opts);
         amqp -> dataflow:add_metrics_handler(metrics_handler_amqp, event_handler_amqp, Opts);
         _ -> ok
      end
      end,
   lists:foreach(F, Handlers).

install_conn_status_handler() ->
   %% standard handler
%%   dataflow:add_conn_status_handler(),
   Handlers = proplists:get_value(handler, faxe_config:get(conn_status, [{handler, []}])),
   lager:notice("conn_status HAndlers: ~p",[Handlers]),
   F = fun({HandlerType, Opts}) ->
      case HandlerType of
         mqtt -> dataflow:add_conn_status_handler(conn_status_handler_mqtt, event_handler_mqtt, Opts);
         amqp -> dataflow:add_conn_status_handler(conn_status_handler_amqp, event_handler_amqp, Opts);
         _ -> ok
      end
       end,
   lists:foreach(F, Handlers).

install_debug_handler() ->
   %% standard handler
%%   dataflow:add_trace_handler(debug_handler),
   Handlers = proplists:get_value(handler, faxe_config:get(debug_trace, [{handler, []}])),
   lager:notice("debug HAndlers: ~p",[Handlers]),
   F = fun({HandlerType, Opts}) ->
      case HandlerType of
         mqtt -> dataflow:add_trace_handler(debug_handler_mqtt, event_handler_mqtt, Opts);
         amqp -> dataflow:add_trace_handler(debug_handler_amqp, event_handler_amqp, Opts);
         _ -> ok
      end
       end,
   lists:foreach(F, Handlers).


