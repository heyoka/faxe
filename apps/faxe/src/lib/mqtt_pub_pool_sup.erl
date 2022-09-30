%%%-------------------------------------------------------------------
%%% @author heyoka
%%% @copyright (C) 2020, <COMPANY>
%%% @doc
%%% @end
%%%-------------------------------------------------------------------
-module(mqtt_pub_pool_sup).

-behaviour(supervisor).

-export([start_link/0, init/1]).

start_link() ->
  supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
  P = [
    {mqtt_pub_pool_manager, {mqtt_pub_pool_manager, start_link, []},
    permanent, 5000, worker, []}
  ],

  {ok, {#{strategy => one_for_one,
    intensity => 5,
    period => 10},
    P}
  }.
