%%%-------------------------------------------------------------------
%%% @author heyoka
%%% @copyright (C) 2020, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 10. Mär 2020 21:16
%%%-------------------------------------------------------------------
-module(faxe_config).
-author("heyoka").

%% API
-export([
   get/1, get/2,
   q_file/1,
   get_mqtt_ssl_opts/0,
   get_amqp_ssl_opts/0,
   get_ssl_opts/1,
   get_http_ssl_opts/0,
   get_http_tls/0,
   filter_empty_options/1,
   get_esq_opts/0]).

get(Key) ->
   application:get_env(faxe, Key, undefined).

get(Key, Default) ->
   application:get_env(faxe, Key, Default).

%% @doc get the base dir for esq q-files
-spec q_file(tuple()) -> list().
q_file({GraphId, NodeId}) when is_binary(GraphId) andalso is_binary(NodeId) ->
   Esq = faxe_config:get(esq),
   EsqBaseDir = proplists:get_value(base_dir, Esq),
   binary_to_list(
      filename:join([EsqBaseDir, GraphId, NodeId])
   ).

get_esq_opts() ->
   Conf0 = faxe_config:get(esq),
   proplists:delete(base_dir, Conf0).

%% ssl options
get_mqtt_ssl_opts() ->
   get_ssl_opts(mqtt).
get_amqp_ssl_opts() ->
   get_ssl_opts(amqp).
get_ssl_opts(Key) when is_atom(Key) ->
   case faxe_config:get(Key) of
      KeyOpts when is_list(KeyOpts) ->
         SslOpts = proplists:get_value(ssl, KeyOpts, []),
         filter_empty_options(proplists:delete(enable, SslOpts));
      _ -> []
   end.

get_http_tls() ->
   case faxe_config:get(http_api) of
      KeyOpts when is_list(KeyOpts) ->
         proplists:get_value(tls, KeyOpts) =:= [{enable, true}];
      _ -> false
   end.

get_http_ssl_opts() ->
   case faxe_config:get(http_api) of
      KeyOpts when is_list(KeyOpts) ->
         SslOpts = proplists:get_value(ssl, KeyOpts, []),
         Ciphers0 = proplists:get_value(ciphers, KeyOpts, []),
         Ciphers = string:split(string:replace(Ciphers0, " ", "", all), ",", all),
         Additional = [{honor_cipher_order, true}, {ciphers, Ciphers}],
         FilerFun = fun({_K, E}) -> E /= [] andalso E /= "" andalso E /= undefined end,
         lists:filter(FilerFun, SslOpts) ++ Additional;
      _ -> []
   end.


filter_empty_options(Proplist) when is_list(Proplist)->
   lists:filter(
      fun({_Key, Value}) ->
         Value /= [] andalso Value /= undefined andalso Value /= <<>> andalso Value /= undef
      end,
      Proplist).

