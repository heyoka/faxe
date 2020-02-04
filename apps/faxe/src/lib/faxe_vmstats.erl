%%%-------------------------------------------------------------------
%%% @author heyoka
%%% @copyright (C) 2020
%%% @doc
%%%
%%% @end
%%% Created : 03. Feb 2020 20:48
%%%-------------------------------------------------------------------
-module(faxe_vmstats).
-author("heyoka").

-behaviour(gen_server).
-behaviour(vmstats_sink).

%% API
-export([start_link/0, collect/3, called/0]).

%% gen_server callbacks
-export([init/1,
   handle_call/3,
   handle_cast/2,
   handle_info/2,
   terminate/2,
   code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {stats = #{}}).

%%%===================================================================
%%% API
%%%===================================================================
collect(Type, Key, Value) when Type =:= timing; Type =:= gauge; Type =:= counter ->
%%   lager:notice("Type: ~p Key: ~p, Value: ~p",[Type, Key, Value]),
   K = lists:flatten(Key),
   call({store, K, Value}).

called() ->
   call(called).

call(Req) ->
   gen_server:call(?SERVER, Req).



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

handle_call(called, _From, State=#state{stats = Stack}) ->
%%   lager:notice("called: ~p",[lists:reverse((Stack))]),
   {reply, Stack, State};
handle_call({store, K, D}, _From, State=#state{stats = Stack}) ->
   Val =
   case string:find(K, ".memory.") of
      nomatch -> D;
      _Match -> faxe_util:round_float(D/1048576, 2) %% memory bytes to mib
   end,
%%   lager:notice("store ~p, ~p",[K,Val]),
   {reply, ok, State#state{stats = maps:put(K, Val, Stack)}};
handle_call(_Req, _From, State) ->
   {reply, ok, State}.

handle_cast(_Request, State) ->
   {noreply, State}.

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
