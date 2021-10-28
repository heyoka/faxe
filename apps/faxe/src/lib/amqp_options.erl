%%%-------------------------------------------------------------------
%%% @author heyoka
%%% @copyright (C) 2020, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 31. Aug 2020 18:42
%%%-------------------------------------------------------------------
-module(amqp_options).
-author("heyoka").

-include_lib("amqp_client/include/amqp_client.hrl").
%% API
-export([parse/1, parse/2]).

-spec parse(list()|map()) -> #amqp_params_network{}.
parse(Opts) ->
   parse(Opts, #{}).
-spec parse(list()|map(), map()) -> #amqp_params_network{}.
parse([], Acc) -> amqp_params(maps:to_list(Acc));

parse(Opts, Acc) when is_map(Opts) ->
   parse(maps:to_list(Opts), Acc);
parse([{host, Host} | R], Acc) when is_binary(Host) ->
   parse(R, Acc#{host => binary_to_list(Host)});
parse([{host, Host} | R], Acc) when is_list(Host) ->
   parse(R, Acc#{host => Host});
parse([{port, Port} | R], Acc) when is_integer(Port) ->
   parse(R, Acc#{port => Port});
parse([{heartbeat, KeepAlive} | R], Acc) when is_integer(KeepAlive) ->
   parse(R, Acc#{heartbeat => KeepAlive});
parse([{user, User} | R], Acc) when is_binary(User) ->
   parse(R, Acc#{user => User});
parse([{pass, Pass} | R], Acc) when is_binary(Pass) ->
   parse(R, Acc#{pass => Pass});
parse([{vhost, VHost} | R], Acc) when is_binary(VHost) ->
   parse(R, Acc#{vhost => VHost});
parse([{connection_timeout, ConnTimeout} | R], Acc) when is_integer(ConnTimeout) ->
   parse(R, Acc#{connection_timeout => ConnTimeout});
parse([{ssl, false} | R], Acc) ->
   parse(R, Acc#{ssl => false});
parse([{ssl, true} | R], Acc) ->
   Opts = faxe_config:get_ssl_opts(amqp),
   parse(R, Acc#{ssl => true, ssl_opts => Opts});
parse([_ | R], Acc) ->
   parse(R, Acc).


amqp_params(Config) ->
   #amqp_params_network{
      username = proplists:get_value(user, Config, <<"guest">>),
      password = proplists:get_value(pass, Config, <<"guest">>),
      virtual_host = proplists:get_value(vhost, Config, <<"/">>),
      port = proplists:get_value(port, Config),
      host = proplists:get_value(host, Config),
      heartbeat = proplists:get_value(heartbeat, Config, 80),
      connection_timeout = proplists:get_value(connection_timeout, Config, 7000),
      ssl_options = proplists:get_value(ssl_opts, Config, none)
   }.
