%%%-------------------------------------------------------------------
%%% @author heyoka
%%% @copyright (C) 2020, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 12. Feb 2020 20:09
%%% @todo
%%% throughput
%%% num errors
%%% cluster stats
%%%-------------------------------------------------------------------
-module(faxe_stats).
-author("heyoka").

-behaviour(gen_server).

%% API
-export([start_link/0, get_stats/0]).

%% gen_server callbacks
-export([init/1,
   handle_call/3,
   handle_cast/2,
   handle_info/2,
   terminate/2,
   code_change/3]).

-define(SERVER, ?MODULE).
-define(INTERVAL, 5000).

-record(state, {
   stats = #{}
}).

%%%===================================================================
%%% API
%%%===================================================================
get_stats() ->
   gen_server:call(?SERVER, get).

-spec(start_link() ->
   {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link() ->
   gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

-spec(init(Args :: term()) ->
   {ok, State :: #state{}} | {ok, State :: #state{}, timeout() | hibernate} |
   {stop, Reason :: term()} | ignore).
init([]) ->
   erlang:send_after(?INTERVAL, self(), gather),
   {ok, #state{}}.

-spec(handle_call(Request :: term(), From :: {pid(), Tag :: term()},
    State :: #state{}) ->
   {reply, Reply :: term(), NewState :: #state{}} |
   {reply, Reply :: term(), NewState :: #state{}, timeout() | hibernate} |
   {noreply, NewState :: #state{}} |
   {noreply, NewState :: #state{}, timeout() | hibernate} |
   {stop, Reason :: term(), Reply :: term(), NewState :: #state{}} |
   {stop, Reason :: term(), NewState :: #state{}}).
handle_call(get, _From, State=#state{stats = Stats}) ->
   {reply, Stats, State};
handle_call(_Request, _From, State) ->
   {reply, ok, State}.

-spec(handle_cast(Request :: term(), State :: #state{}) ->
   {noreply, NewState :: #state{}} |
   {noreply, NewState :: #state{}, timeout() | hibernate} |
   {stop, Reason :: term(), NewState :: #state{}}).
handle_cast(_Request, State) ->
   {noreply, State}.


-spec(handle_info(Info :: timeout() | term(), State :: #state{}) ->
   {noreply, NewState :: #state{}} |
   {noreply, NewState :: #state{}, timeout() | hibernate} |
   {stop, Reason :: term(), NewState :: #state{}}).
handle_info(gather, State = #state{stats = Stats}) ->
   erlang:send_after(?INTERVAL, self(), gather),
   {ok, FaxeVsn} = application:get_key(faxe, vsn),
   TasksAll = faxe:list_tasks(),
   TasksRunning = faxe:list_running_tasks(),
   TasksTemp = faxe:list_temporary_tasks(),
   TasksPermanent = faxe:list_permanent_tasks(),
   TemplatesAll = faxe:list_templates(),
   Paths = ets:tab2list(field_paths),
   S = #{
      <<"data_throughput_sec">> => faxe_lambda_lib:random(203),
      <<"data_paths_known">> => length(Paths),
      <<"faxe_version">> => list_to_binary(FaxeVsn),
      <<"opt_version">> => list_to_binary(faxe_util:get_erlang_version()),
      <<"registered_tasks">> => length(TasksAll),
      <<"running_tasks">> => length(TasksRunning),
      <<"running_temp_tasks">> => length(TasksTemp),
      <<"permanent_tasks">> => length(TasksPermanent),
      <<"registered_templates">> => length(TemplatesAll)
   },
%%   lager:notice("~p collected: ~p",[?SERVER, S]),
   {noreply, State#state{stats = maps:merge(Stats, S)}};
handle_info(_Info, State) ->
   {noreply, State}.

-spec(terminate(Reason :: (normal | shutdown | {shutdown, term()} | term()),
    State :: #state{}) -> term()).
terminate(_Reason, _State) ->
   ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
-spec(code_change(OldVsn :: term() | {down, term()}, State :: #state{},
    Extra :: term()) ->
   {ok, NewState :: #state{}} | {error, Reason :: term()}).
code_change(_OldVsn, State, _Extra) ->
   {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
