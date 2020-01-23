%%%-------------------------------------------------------------------
%%% @author heyoka
%%% @copyright (C) 2020, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 23. Jan 2020 10:58
%%%-------------------------------------------------------------------
-module(reconnect_watcher).
-author("heyoka").

-behaviour(gen_server).

%% API
-export([start_link/0, new/2, new/3, bump/0]).

%% gen_server callbacks
-export([init/1,
   handle_call/3,
   handle_cast/2,
   handle_info/2,
   terminate/2,
   code_change/3]).

-define(SERVER, ?MODULE).

-record(recon_watch, {
   window = 10000 :: non_neg_integer(), %% time window in milliseconds
   recon_count = 10 :: non_neg_integer(), %% max recons during time window
   warn_message = "more than ~p reconnects in ~p seconds ( ~p )" :: list(),
   count = 0 :: non_neg_integer() %% actual counter
}).

-record(state, {
   timer_refs = [] :: list({pid(), reference()}),
   watched = [] :: list({pid(), #recon_watch{}})
}).

%%%===================================================================
%%% API
%%%===================================================================
new(Window, Count) ->
   new(Window, Count, "").
new(Window, Count, InfoString) when is_integer(Window), is_integer(Count), is_list(InfoString) ->
   gen_server:cast(?SERVER, {new, Window, Count, InfoString, self()}).

bump() ->
   gen_server:cast(?SERVER, {bump, self()}).

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @end
%%--------------------------------------------------------------------
-spec(start_link() ->
   {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link() ->
   gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
-spec(init(Args :: term()) ->
   {ok, State :: #state{}} | {ok, State :: #state{}, timeout() | hibernate} |
   {stop, Reason :: term()} | ignore).
init([]) ->
   {ok, #state{}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @end
%%--------------------------------------------------------------------
-spec(handle_call(Request :: term(), From :: {pid(), Tag :: term()},
    State :: #state{}) ->
   {reply, Reply :: term(), NewState :: #state{}} |
   {reply, Reply :: term(), NewState :: #state{}, timeout() | hibernate} |
   {noreply, NewState :: #state{}} |
   {noreply, NewState :: #state{}, timeout() | hibernate} |
   {stop, Reason :: term(), Reply :: term(), NewState :: #state{}} |
   {stop, Reason :: term(), NewState :: #state{}}).
handle_call(_Request, _From, State) ->
   {reply, ok, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @end
%%--------------------------------------------------------------------
-spec(handle_cast(Request :: term(), State :: #state{}) ->
   {noreply, NewState :: #state{}} |
   {noreply, NewState :: #state{}, timeout() | hibernate} |
   {stop, Reason :: term(), NewState :: #state{}}).
handle_cast({new, Window, Count, Info, Pid}, State = #state{watched = Watches}) ->
   erlang:monitor(process, Pid),
   NewState = cancel_timer(Pid, State),
   NewWatch = #recon_watch{window = Window, recon_count = Count},
   Message = io_lib:format(NewWatch#recon_watch.warn_message,
      [Count, round(Window/1000), lists:flatten(Info)]),
   Watches0 = proplists:delete(Pid, Watches),
   NewWatches = [{Pid, NewWatch#recon_watch{warn_message = Message}}|Watches0],
   {noreply, NewState#state{watched = NewWatches}};
handle_cast({bump, Pid}, State = #state{watched = Watches}) ->
   NewState =
   case proplists:get_value(Pid, Watches) of
      undefined -> State;
      W = #recon_watch{count = Count, recon_count = MaxCount, warn_message = Msg, window = Win} ->
         NewCount = Count+1,
%%         lager:notice("new count for ~p is :~p",[Pid, NewCount]),
         case NewCount > MaxCount of
            true ->
               lager:warning(Msg),
               State1 = cancel_timer(Pid, State),
               NewWatch = W#recon_watch{count = 0},
               State1#state{watched = [{Pid,NewWatch}|proplists:delete(Pid, Watches)]};
            false ->
               NewWatch = W#recon_watch{count = NewCount},
               State1 = maybe_start_timer(Pid, Win, State),
               State1#state{watched = [{Pid, NewWatch}|proplists:delete(Pid, Watches)]}
         end
   end,
   {noreply, NewState};
handle_cast(_Request, State) ->
   {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
-spec(handle_info(Info :: timeout() | term(), State :: #state{}) ->
   {noreply, NewState :: #state{}} |
   {noreply, NewState :: #state{}, timeout() | hibernate} |
   {stop, Reason :: term(), NewState :: #state{}}).
handle_info({'DOWN', _MonitorRef, process, Pid, _Info}, State=#state{watched = Watches}) ->
%%   lager:notice("Pid: ~p is DOWN",[Pid]),
   NewState = cancel_timer(Pid, State),
   NewWatches = proplists:delete(Pid, Watches),
%%   lager:info("new watches are: ~p", [NewWatches]),
   {noreply, NewState#state{watched = NewWatches}};
%% reset the counter
handle_info({win_timeout, Pid}, State = #state{watched = Watches, timer_refs = TRefs}) ->
%%   lager:info("win_timeout for pid: ~p",[Pid]),
   NewTimers = proplists:delete(Pid, TRefs),
   NewState =
   case proplists:get_value(Pid, Watches) of
      undefined ->
         State;
      W=#recon_watch{} ->
%%         lager:info("reset counter for pid: ~p",[Pid]),
         NewW = W#recon_watch{count = 0},
         State#state{watched = [{Pid, NewW}|proplists:delete(Pid, Watches)]}
   end,
   {noreply, NewState#state{timer_refs = NewTimers}};
handle_info(_Info, State) ->
   {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
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
maybe_start_timer(Pid, Window, State = #state{timer_refs = TRefs}) ->
   case proplists:get_value(Pid, TRefs) of
      undefined ->
%%         lager:info("start new timer for ~p [~p]",[Pid, Window]),
         Timer = erlang:send_after(Window, self(), {win_timeout, Pid}),
         State#state{timer_refs = [{Pid, Timer}|TRefs]};
      _Timer -> State
   end.


cancel_timer(Pid, State=#state{timer_refs = TRefs}) ->
   cancel(proplists:get_value(Pid, TRefs)),
   NewTRefs = proplists:delete(Pid, TRefs),
   State#state{timer_refs = NewTRefs}.

cancel(undefined) -> ok;
cancel(Timer) when is_reference(Timer) -> erlang:cancel_timer(Timer).