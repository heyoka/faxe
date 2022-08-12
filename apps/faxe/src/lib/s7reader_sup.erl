%%%-------------------------------------------------------------------
%%% @author heyoka
%%% @copyright (C) 2022, <COMPANY>
%%% @doc
%%% @end
%%%-------------------------------------------------------------------
-module(s7reader_sup).

-behaviour(supervisor).

-export([start_link/0, init/1, start_reader/1]).

start_reader(Opts) ->
  supervisor:start_child(?MODULE, child(Opts)).

start_link() ->
  supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
  {ok, {#{strategy => one_for_one,
    intensity => 5,
    period => 30},
    []}
  }.

child(Opts = #{ip := Ip}) ->
  #{id => Ip,
    start => {s7reader, start_link, [Opts]},
    restart => transient,
    type => worker,
    shutdown => 4000}.