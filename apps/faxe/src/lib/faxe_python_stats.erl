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
-module(faxe_python_stats).
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
-define(INTERVAL, 15000).

-record(state, {
   python :: undefined|pid(),
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
   Python = c_python3:get_python(undefined),
   erlang:send_after(?INTERVAL, self(), gather),
   {ok, #state{python = Python}}.

-spec(handle_call(Request :: term(), From :: {pid(), Tag :: term()},
    State :: #state{}) ->
   {reply, Reply :: term(), NewState :: #state{}} |
   {reply, Reply :: term(), NewState :: #state{}, timeout() | hibernate} |
   {noreply, NewState :: #state{}} |
   {noreply, NewState :: #state{}, timeout() | hibernate} |
   {stop, Reason :: term(), Reply :: term(), NewState :: #state{}} |
   {stop, Reason :: term(), NewState :: #state{}}).
handle_call(get, _From, State=#state{stats = #{<<"proc_list">> := ProcList0} = Stats}) ->
   ProcList1 = lists:sort(fun(#{<<"mem">> := A}, #{<<"mem">> := B}) -> A > B end, ProcList0),
   ProcList = lists:sublist(ProcList1, 20),
   {reply, {ok, Stats#{<<"proc_list">> => ProcList}}, State};
handle_call(_Request, _From, State=#state{stats = Stats}) ->
   {reply, {ok, #{}}, State}.

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
handle_info(gather, State = #state{python = Python}) ->
   erlang:send_after(?INTERVAL, self(), gather),
   PythonNodes = ets:tab2list(python_procs),
   PPids = [{Pid, iolist_to_binary([FId, "-", NId])}
      || {_P, #{<<"os_pid">> := Pid, <<"flownode">> := {FId, NId}}} <- PythonNodes],
   {ok, ProcessStats} = pythra:pythra_call(Python, 'faxe_handler', 'py_stats', [maps:from_list(PPids)]),
   %% #{<<"mem_total">> := Mem, <<"cpu_total">> := CpuPercent, <<"proc_list">> := []}
   {noreply, State#state{stats = ProcessStats#{<<"nodes_running">> => length(PPids)}}};
handle_info(_Info, State) ->
   {noreply, State}.

-spec(terminate(Reason :: (normal | shutdown | {shutdown, term()} | term()),
    State :: #state{}) -> term()).
terminate(_Reason, #state{python = Python}) ->
   python:stop(Python),
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
