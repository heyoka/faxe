%%%-------------------------------------------------------------------
%% @doc faxe top level supervisor.
%% @end
%%%-------------------------------------------------------------------

-module(faxe_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

%%====================================================================
%% API functions
%%====================================================================

start_link() ->
   supervisor:start_link({local, ?SERVER}, ?MODULE, []).

%%====================================================================
%% Supervisor callbacks
%%====================================================================

%% Child :: {Id,StartFunc,Restart,Shutdown,Type,Modules}
init([]) ->
    Procs = [
%%        {faxe_peer_manager,
%%            {faxe_peer_manager, start_link, []},
%%            permanent, 5000, worker, []},

        {dataflow_sup,
            {dataflow_sup, start_link, []},
            permanent, infinity, supervisor, [dataflow_sup]},
        {faxe_ets,
            {faxe_ets, start_link, []},
            permanent, 5000, worker, []},
        {faxe_vmstats,
            {faxe_vmstats, start_link, []},
            permanent, 5000, worker, []},
        {faxe_stats,
            {faxe_stats, start_link, []},
            permanent, 5000, worker, []},
        {reconnect_watcher,
            {reconnect_watcher, start_link, []},
            permanent, 5000, worker, []},
        {faxe_log_sup,
            {faxe_log_sup, start_link, []},
            permanent, infinity, supervisor, [faxe_log_sup]}
%%        {initial_task_starter,
%%            {initial_task_starter, start_link, []},
%%            permanent, 5000, worker, []}
   ],
   {ok, { {one_for_one, 5, 10}, Procs} }.

%%====================================================================
%% Internal functions
%%====================================================================
