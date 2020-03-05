%%%-------------------------------------------------------------------
%%% @author heyoka
%%% @copyright (C) 2020, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 05. Mär 2020 19:37
%%%-------------------------------------------------------------------
-module(faxe_log_sup).
-author("heyoka").

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

%%%===================================================================
%%% API functions
%%%===================================================================

%% @doc Starts the supervisor
-spec(start_link() -> {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link() ->
   supervisor:start_link({local, ?SERVER}, ?MODULE, []).

%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================

%% @private
%% @doc Whenever a supervisor is started using supervisor:start_link/[2,3],
%% this function is called by the new process to find out about
%% restart strategy, maximum restart frequency and child
%% specifications.
-spec(init(Args :: term()) ->
   {ok, {SupFlags :: {RestartStrategy :: supervisor:strategy(),
      MaxR :: non_neg_integer(), MaxT :: non_neg_integer()},
      [ChildSpec :: supervisor:child_spec()]}}
   | ignore | {error, Reason :: term()}).
init([]) ->
   MaxRestarts = 10,
   MaxSecondsBetweenRestarts = 20,
   SupFlags = #{strategy => one_for_one,
      intensity => MaxRestarts,
      period => MaxSecondsBetweenRestarts},
   Procs = log_procs(),

   {ok, {SupFlags, Procs}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
log_procs() ->
   {ok, Lager} = application:get_env(lager, handlers),
   case proplists:get_value(lager_flowlog_backend, Lager) of
      undefined -> [];
      LogBackend -> case proplists:get_value(storage_backend, LogBackend) of
                       crate_log_writer -> [
                          {crate_log_writer,
                             {crate_log_writer, start_link, []},
                             permanent, 5000, worker, []},
                          {crate_log_reader,
                             {crate_log_reader, start_link, []},
                             permanent, 5000, worker, []}
                       ];
                       _ -> []
                    end
   end.
