%%%-------------------------------------------------------------------
%%% @author heyoka
%%% @copyright (C) 2020, <COMPANY>
%%% @doc
%%% @end
%%%-------------------------------------------------------------------
-module(faxe_event_guard_sup).

-behaviour(supervisor).

-export([start_link/0, init/1]).

start_link() ->
  supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
  P = [
    % event handler guard process
    {faxe_event_guard , {faxe_event_guard, start_link, []},
      permanent, 5000, worker, [faxe_event_guard]}
  ],

  {ok, {#{strategy => simple_one_for_one,
    intensity => 10,
    period => 60},
    P}
  }.
