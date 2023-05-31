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
-module(faxe_s7_stats).
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
-define(INTERVAL, 20000).

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
   {reply, #{<<"s7_pools">> => Stats}, State};
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
handle_info(gather, State = #state{}) ->
   erlang:send_after(?INTERVAL, self(), gather),
   S7Pools = lists:map(
      fun({Key1, Conns1}) ->
         Ip = faxe_util:to_bin(Key1),
         S7Clients = s7pool_manager:get_clients(Key1),
         #{<<"reader_stats">> => s7reader:get_stats(Ip), <<"peer">> => Ip,
            <<"num_connections">> => length(Conns1), <<"clients">> => length(S7Clients)}
      end,
      ets:tab2list(s7_pools)),
   {noreply, State#state{stats = S7Pools}};
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
