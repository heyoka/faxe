%%%-------------------------------------------------------------------
%%% @author heyoka
%%% @copyright (C) 2020, <COMPANY>
%%% @doc
%%% @end
%%%-------------------------------------------------------------------
-module(faxe_event_sup).

-behaviour(supervisor).

-export([start_link/0, init/1]).

start_link() ->
  supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
  P = [
    % event handler guard supervisor
    {faxe_event_guard_sup, {faxe_event_guard_sup, start_link, []},
      permanent, 5000, supervisor, [faxe_event_guard_sup]}
    ,
    {faxe_metrics,
      {gen_event, start_link, [{local, faxe_metrics}]},
      permanent, 5000, worker, []}
    ,
    {conn_status,
      {gen_event, start_link, [{local, conn_status}]},
      permanent, 5000, worker, []}
    ,
    {faxe_debug,
      {gen_event, start_link, [{local, faxe_debug}]},
      permanent, 5000, worker, []}
    ,
    {flow_changed,
      {gen_event, start_link, [{local, flow_changed}]},
      permanent, 5000, worker, []}

  ],

  {ok, {#{strategy => one_for_one,
    intensity => 10,
    period => 60},
    P}
  }.
