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
   install_handlers(debug),
   install_handlers(conn_status),
   install_handlers(metrics),
   print_started(HttpPort),
   Res.

%%--------------------------------------------------------------------
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
install_metrics_handler() ->
   %% standard handler
%%   dataflow:add_metrics_handler(),
   Handlers = proplists:get_value(handler, faxe_config:get(metrics, [{handler, []}] )),
%%   lager:notice("Metric Handlers: ~p",[Handlers]),
   F = fun({HandlerType, Opts}) ->
      case HandlerType of
         mqtt ->
            lager:notice("Metric Handler MQTT: ~p",[jiffy:encode(maps:from_list(Opts))]),
            dataflow:add_metrics_handler(metrics_handler_mqtt, event_handler_mqtt, Opts);
         amqp ->
            lager:notice("Metric Handler AMQP: ~p",[jiffy:encode(maps:from_list(Opts))]),
            dataflow:add_metrics_handler(metrics_handler_amqp, event_handler_amqp, Opts);
         _ -> ok
      end
      end,
   lists:foreach(F, Handlers).

install_conn_status_handler() ->
   %% standard handler
%%   dataflow:add_conn_status_handler(),
   Handlers = proplists:get_value(handler, faxe_config:get(conn_status, [{handler, []}])),
%%   lager:notice("conn_status Handlers: ~p",[Handlers]),
   F = fun({HandlerType, Opts}) ->
      case HandlerType of
         mqtt ->
            lager:notice("Conn Status Handler MQTT: ~p",[jiffy:encode(maps:from_list(Opts))]),
            dataflow:add_conn_status_handler(conn_status_handler_mqtt, event_handler_mqtt, Opts);
         amqp ->
            lager:notice("Conn Status Handler AMQP: ~p",[jiffy:encode(maps:from_list(Opts))]),
            dataflow:add_conn_status_handler(conn_status_handler_amqp, event_handler_amqp, Opts);
         _ -> ok
      end
       end,
   lists:foreach(F, Handlers).

%%install_debug_handler() ->
%%   Handlers = get_enabled(debug, handler),
%%      proplists:get_value(handler, faxe_config:get(debug_trace, [{handler, []}])),
%%   lager:notice("debug Handlers: ~p",[Handlers]), || {
%%   [add_handler(HandlerType, debug, Opts) || {HandlerType, Opts} <- Handlers].
%%   F = fun({HandlerType, Opts}) -> add_handler(HandlerType, <<"debug">>, Opts) end,
%%   lists:foreach(F, Handlers).



install_handlers(Key) ->
   HandlersMqtt = get_enabled(Key, handler, mqtt),
   [add_handler(HandlerType, atom_to_binary(Key, utf8), Opts) || {HandlerType, Opts} <- HandlersMqtt],
   HandlersAmqp = get_enabled(Key, handler, amqp),
   [add_handler(HandlerType, atom_to_binary(Key, utf8), Opts) || {HandlerType, Opts} <- HandlersAmqp].

%% from a config proplist get only those with an 'enable' flag set to 'true'
get_enabled(Key, SubKey, SubSubKey) ->
   All = faxe_config:get(Key, []),
   lager:notice("All: ~p", [All]),
   case proplists:get_value(SubKey, All) of
      undefined -> [];
      [] -> [];
      AllType when is_list(AllType) ->
         case proplists:get_value(SubSubKey, AllType) of
            undefined -> [];
            List when is_list(List) ->
               lager:notice("The list: ~p",[List]),
               lists:filter(fun(E) -> proplists:get_value(enable, E) end, List)
         end
   end
   .

add_handler(mqtt, HandlerName, Opts) ->
   lager:notice("~p Handler MQTT: ~p",[handler_name(HandlerName, <<"mqtt">>), jiffy:encode(maps:from_list(Opts))]),
   dataflow:add_trace_handler(handler_name(HandlerName,<<"mqtt">>), event_handler_mqtt, Opts);
add_handler(amqp, HandlerName, Opts) ->
   lager:notice("~p Handler AMQP: ~p",[handler_name(HandlerName, <<"amqp">>), jiffy:encode(maps:from_list(Opts))]),
   dataflow:add_trace_handler(handler_name(HandlerName, <<"amqp">>), event_handler_amqp, Opts).

handler_name(Name, Type) ->
   binary_to_atom(<<Name/binary, "_handler_", Type/binary>>, utf8).

