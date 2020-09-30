%%%-------------------------------------------------------------------
%%% @author heyoka
%%% @copyright (C) 2020
%%% @doc
%%%
%%% @end
%%% Created : 02. Sep 2020 14:24
%%%-------------------------------------------------------------------
-module(faxe_event_handlers).
-author("heyoka").

%% API
-export([install/0]).

install() ->
  install_handlers(debug),
  install_handlers(conn_status),
  install_handlers(metrics).

install_handlers(Key) ->
  Handlers = get_enabled(Key, handler),
  [add_handler(HandlerType, atom_to_binary(Key, utf8), Opts) || {HandlerType, Opts} <- Handlers].

%% from a config proplist get only those with an 'enable' flag set to 'true'
get_enabled(Key, SubKey) ->
  All = faxe_config:get(Key, []),
  case proplists:get_value(SubKey, All) of
    undefined -> [];
    List when is_list(List) ->
      lists:filter(fun({_Type, E}) -> proplists:get_value(enable, E) end, List)
  end
.

add_handler(mqtt, HandlerName, Opts) ->
  MqttOpts = faxe_util:proplists_merge(filter_options(Opts), faxe_config:get(mqtt, [])),
%%  lager:notice("~p Handler MQTT: ~p",[handler_name(HandlerName, <<"mqtt">>), MqttOpts]),
  add_event_handler(HandlerName, handler_name(HandlerName,<<"mqtt">>), event_handler_mqtt, MqttOpts);
add_handler(amqp, HandlerName, Opts) ->
  AmqpOpts = faxe_util:proplists_merge(filter_options(Opts), faxe_config:get(amqp, [])),
%%  lager:notice("~p Handler AMQP: ~p",[handler_name(HandlerName, <<"amqp">>), AmqpOpts]),
  add_event_handler(HandlerName, handler_name(HandlerName, <<"amqp">>), event_handler_amqp, AmqpOpts).


add_event_handler(<<"conn_status">>, Name, Type, Args) ->
  dataflow:add_conn_status_handler(Name, Type, Args);
add_event_handler(<<"debug">>, Name, Type, Args) ->
  dataflow:add_trace_handler(Name, Type, Args);
add_event_handler(<<"metrics">>, Name, Type, Args) ->
  dataflow:add_metrics_handler(Name, Type, Args).


handler_name(Name, Type) ->
  binary_to_atom(<<Name/binary, "_handler_", Type/binary>>, utf8).

filter_options(Proplist) ->
  lists:filter(
    fun({_Key, Value}) ->
      Value /= [] andalso Value /= undefined andalso Value /= <<>> andalso Value /= undef
    end,
    Proplist).
