%%%-------------------------------------------------------------------
%%% @author heyoka
%%% @copyright (C) 2019, <COMPANY>
%%% @doc
%%% there are two modes for this server, either it  consumes from a persistent esq queue and publishes to a mqtt broker or
%%% it waits for a message from another process to directly publish messages to a broker
%%% @end
%%% Created : 26. Jul 2019 11:41
%%%-------------------------------------------------------------------
-module(mqtt_publisher).
-author("heyoka").

-behaviour(gen_server).

%% API
-export([start_link/2, start_link/1]).

%% gen_server callbacks
-export([init/1,
   handle_call/3,
   handle_cast/2,
   handle_info/2,
   terminate/2,
   code_change/3]).

-record(state, {
   client,
   connected = false,
   host,
   port,
   user,
   pass,
   qos,
   retained = false,
   ssl = false,
   queue,
   mem_queue,
   deq_interval = 15,
   reconnector
}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @end
%%--------------------------------------------------------------------
-spec(start_link(map(), pid()) ->
   {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link(Opts, Queue) ->
   gen_server:start_link( ?MODULE, [Opts, Queue], []).

start_link(Opts) ->
   gen_server:start_link( ?MODULE, [Opts], []).

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
init([#{} = Opts]) ->
   init_all(Opts, #state{mem_queue = queue:new()});
init([#{} = Opts, Queue]) ->
   init_all(Opts, #state{queue = Queue}).

init_all(#{host := Host, port := Port, user := User, pass := Pass,
      retained := Retained, ssl := UseSSL, qos := Qos}, State) ->
   process_flag(trap_exit, true),
   reconnect_watcher:new(10000, 5, io_lib:format("~s:~p ~p",[Host, Port, ?MODULE])),
   Reconnector = faxe_backoff:new({5, 1200}),
   {ok, Reconnector1} = faxe_backoff:execute(Reconnector, reconnect),
   {ok,
      State#state{
         host = Host, port = Port, user = User, pass = Pass, reconnector = Reconnector1,
         retained = Retained, ssl = UseSSL, qos = Qos}}.

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
handle_info({mqttc, C, connected}, State=#state{queue = undefined, mem_queue = Q}) ->
   PendingList = queue:to_list(Q),
   NewState = State#state{client = C, connected = true, mem_queue = queue:new()},
   [publish(M, NewState) || M <- PendingList],
   lager:info("mqtt client connected!!"),
   {noreply, NewState};
handle_info({mqttc, C, connected}, State=#state{}) ->
   lager:info("mqtt client connected!!"),
   NewState = State#state{client = C, connected = true},
   next(NewState),
   {noreply, NewState};
handle_info({mqttc, _C,  disconnected}, State=#state{client = Client}) ->
   lager:debug("mqtt client disconnected!!"),
   catch exit(Client, kill),
   {noreply, State#state{connected = false, client = undefined}};
handle_info(deq, State=#state{}) ->
   next(State),
   {noreply, State};
handle_info({publish, {Topic, Message}}, State = #state{connected = false, mem_queue = Q}) ->
   Q1 = queue:in({Topic, Message}, Q),
   {noreply, State#state{mem_queue = Q1}};
handle_info({publish, {Topic, Message}}, State = #state{}) ->
   publish({Topic, Message}, State),
   {noreply, State};
handle_info(reconnect, State = #state{}) ->
%%   lager:notice("(re)connect to : ~p",[State#state.host]),
   NewState = do_connect(State),
   {noreply, NewState};
handle_info({'EXIT', _Client, Reason},
    State = #state{reconnector = Recon, host = H, port = P}) ->
   lager:notice("MQTT Client exit: ~p ~p", [Reason, {H, P}]),
   {ok, Reconnector} = faxe_backoff:execute(Recon, reconnect),
   {noreply, State#state{connected = false, client = undefined, reconnector = Reconnector}};
handle_info(E, S) ->
   lager:warning("unexpected: ~p~n", [E]),
   {noreply, S}.


next(State=#state{queue = Q, deq_interval = Interval}) ->
   case esq:deq(Q) of
      [] -> ok; %lager:info("Queue is empty!"), ok;
      [#{payload := {_Topic, _Message}=M}] ->
         lager:notice("~p: msg from Q: ~p", [faxe_time:now(), M]),
         publish(M, State)
   end,
   erlang:send_after(Interval, self(), deq).


publish({Topic, Msg}, #state{retained = Ret, qos = Qos, client = C})
   when is_binary(Msg); is_list(Msg) ->
   lager:notice("publish ~s on topic ~p ~n",[Msg, Topic]),
   ok = emqttc:publish(C, Topic, Msg, [{qos, Qos}, {retain, Ret}]).

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
terminate(_Reason, #state{client = C}) ->
   catch (emqttc:disconnect(C)).

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
do_connect(#state{host = Host, port = Port, user = User, pass = Pass, ssl = _Ssl} = State) ->
   reconnect_watcher:bump(),
   Opts = [{host, Host}, {port, Port}, {keepalive, 30}, {username, User}, {password, Pass}],
   {ok, _Client} = emqttc:start_link(Opts),
   State.