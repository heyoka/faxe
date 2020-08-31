%%%-------------------------------------------------------------------
%%% @author heyoka
%%% @copyright (C) 2020, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 31. Aug 2020 18:42
%%%-------------------------------------------------------------------
-module(mqtt_options).
-author("heyoka").

%% API
-export([parse/1, parse/2]).

-spec parse(list()|map()) -> list().
parse(Opts) ->
   parse(Opts, #{}).
-spec parse(list()|map(), map()) -> list().
parse([], Acc) -> maps:to_list(Acc);
parse(Opts, Acc) when is_list(Opts) ->
   parse(maps:from_list(Opts), Acc);
parse([{host, Host} | R], Acc) when is_binary(Host) ->
   parse(R, Acc#{host => binary_to_list(Host)});
parse([{host, Host} | R], Acc) when is_list(Host) ->
   parse(R, Acc#{host => Host});
parse([{port, Port} | R], Acc) when is_integer(Port) ->
   parse(R, Acc#{port => Port});
parse([{keepalive, KeepAlive} | R], Acc) when is_integer(KeepAlive) ->
   parse(R, Acc#{keepalive => KeepAlive});
parse([{user, User} | R], Acc) when is_binary(User) ->
   parse(R, Acc#{user => User});
parse([{pass, Pass} | R], Acc) when is_binary(Pass) ->
   parse(R, Acc#{pass => Pass});
parse([{retained, Ret} | R], Acc) when is_atom(Ret) ->
   parse(R, Acc#{retained => Ret});
parse([{qos, Qos} | R], Acc) when is_integer(Qos) ->
   parse(R, Acc#{qos => Qos});
parse([{client_id, ClientId} | R], Acc) when is_binary(ClientId) ->
   parse(R, Acc#{client_id => ClientId});
parse([{ssl, false} | R], Acc) ->
   parse(R, Acc#{ssl => false});
parse([{ssl, true} | R], Acc) ->
   Opts = faxe_config:get_ssl_opts(mqtt),
   parse(R, Acc#{ssl => true, ssl_opts => Opts});
parse([_ | R], Acc) ->
   parse(R, Acc).


