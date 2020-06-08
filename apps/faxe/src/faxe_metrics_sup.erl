%%%-------------------------------------------------------------------
%%% @author heyoka
%%% @copyright (C) 2020, <COMPANY>
%%% @doc
%%% @end
%%%-------------------------------------------------------------------
-module(faxe_metrics_sup).

-behaviour(supervisor).

-export([start_link/0, init/1]).

start_link() ->
  supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
  P = [
    {connection_registry,
      {connection_registry, start_link, []},
      permanent, 5000, worker, []}
    ,
    {metrics_collector,
      {metrics_collector, start_link, []},
      permanent, 5000, worker, []}
    ,
    {faxe_metrics,
      {gen_event, start_link, [{local, faxe_metrics}]},
      permanent, 5000, worker, []}
    ,
    {conn_status,
      {gen_event, start_link, [{local, conn_status}]},
      permanent, 5000, worker, []}
  ],

  {ok, {#{strategy => one_for_one,
    intensity => 5,
    period => 10},
    P}
  }.
