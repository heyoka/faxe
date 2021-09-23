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
   ssl_opts = [],
   queue,
   mem_queue :: memory_queue:mem_queue(),
   max_mem_queue_len = 100,
   reconnector :: faxe_backoff:backoff(),
   node_id,
   client_id,
   adapt_interval :: adaptive_interval:interval()
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
   init_all(Opts, #state{mem_queue = memory_queue:new()});
init([#{} = Opts, Queue]) ->
   init_all(Opts, #state{queue = Queue, adapt_interval = adaptive_interval:new()}).

init_all(#{host := Host, port := Port} = Opts, State) ->
%%   lager:info("[~p] MQTT_OPTS are: ~p",[?MODULE, Opts]),
   NId =
   case maps:is_key(node_id, Opts) of
      true -> maps:get(node_id, Opts);
      false -> {<<"sys">>, <<"sys">>}
   end,

   process_flag(trap_exit, true),
   reconnect_watcher:new(10000, 5, io_lib:format("~s:~p ~p",[Host, Port, ?MODULE])),
   Reconnector = faxe_backoff:new({100, 4200}),
   connection_registry:reg(NId, Host, Port, <<"mqtt">>),
   {ok, Reconnector1} = faxe_backoff:execute(Reconnector, reconnect),
   connection_registry:connecting(),
   OptsState = init_opts(Opts, State),
   {ok, OptsState#state{reconnector = Reconnector1, node_id = NId }}.


init_opts([], State) -> State;
init_opts(Opts, State) when is_map(Opts) ->
   init_opts(maps:to_list(Opts), State);
init_opts([{host, Host} | R], State) when is_binary(Host) ->
   init_opts(R, State#state{host = binary_to_list(Host)});
init_opts([{host, Host} | R], State) when is_list(Host) ->
   init_opts(R, State#state{host = Host});
init_opts([{port, Port} | R], State) when is_integer(Port) ->
   init_opts(R, State#state{port = Port});
init_opts([{user, User} | R], State) when is_binary(User) ->
   init_opts(R, State#state{user = User});
init_opts([{pass, Pass} | R], State) when is_binary(Pass) ->
   init_opts(R, State#state{pass = Pass});
init_opts([{retained, Ret} | R], State) when is_atom(Ret) ->
   init_opts(R, State#state{retained = Ret});
init_opts([{qos, Qos} | R], State) when is_integer(Qos) ->
   init_opts(R, State#state{qos = Qos});
init_opts([{max_mem_queue_size, QLen} | R], State) when is_integer(QLen) andalso QLen > 0 ->
   init_opts(R, State#state{max_mem_queue_len = QLen});
init_opts([{client_id, ClientId} | R], State) when is_binary(ClientId) ->
   init_opts(R, State#state{client_id = ClientId});
init_opts([{ssl, false} | R], State) ->
   init_opts(R, State#state{ssl = false});
init_opts([{ssl, true} | R], State) ->
   Opts = faxe_config:get_mqtt_ssl_opts(),
   init_opts(R, State#state{ssl = true, ssl_opts = Opts});
init_opts([_ | R], State) ->
   init_opts(R, State).

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
%% mem-queue only
handle_info({mqttc, C, connected},
    State=#state{queue = undefined, mem_queue = Q, host = Host}) ->
   connection_registry:connected(),
   {PendingList, NewQ} = memory_queue:to_list_reset(Q),
   NewState = State#state{client = C, connected = true, mem_queue = NewQ},
   [publish(M, NewState) || M <- PendingList],
   lager:info("mqtt client connected to ~p",[Host]),
   {noreply, NewState};
%% internal ondisc queue is used
handle_info({mqttc, C, connected}, State=#state{}) ->
   connection_registry:connected(),
   lager:info("mqtt client connected!!"),
   NewState = next(State#state{client = C, connected = true}),
   {noreply, NewState};
handle_info({mqttc, _C,  disconnected}, State=#state{client = Client}) ->
   connection_registry:disconnected(),
   lager:debug("mqtt client disconnected!!"),
   catch exit(Client, kill),
   {noreply, State#state{connected = false, client = undefined}};
handle_info(deq, State=#state{}) ->
   {noreply, next(State)};
handle_info({publish, {_Topic, _Message}=M}, State = #state{connected = false, mem_queue = Q}) ->
   NewQ = memory_queue:enq(M, Q),
   {noreply, State#state{mem_queue = NewQ}};
%%   Q1 = queue:in({Topic, Message}, Q),
%%   {noreply, State#state{mem_queue = Q1}};
handle_info({publish, {Topic, Message}}, State = #state{}) ->
   publish({Topic, Message}, State),
   {noreply, State};
handle_info(reconnect, State = #state{}) ->
%%   lager:notice("(re)connect to : ~p",[State#state.host]),
   NewState = do_connect(State),
   {noreply, NewState};
handle_info({'EXIT', _Client, Reason}, State = #state{reconnector = Recon, host = H, port = P}) ->
   connection_registry:disconnected(),
   lager:notice("MQTT Client exit: ~p ~p", [Reason, {H, P}]),
   {ok, Reconnector} = faxe_backoff:execute(Recon, reconnect),
   {noreply, State#state{connected = false, client = undefined, reconnector = Reconnector}};
handle_info(E, S) ->
   lager:warning("unexpected: ~p~n", [E]),
   {noreply, S}.

next(State=#state{queue = Q, adapt_interval = AdaptInt}) ->
   {NewInterval, NewAdaptInt} =
   case esq:deq(Q) of
      [] ->
         adaptive_interval:in(miss, AdaptInt);
      [#{payload := {_Topic, _Message}=M}] ->
         publish(M, State),
         adaptive_interval:in(hit, AdaptInt)
   end,
   erlang:send_after(NewInterval, self(), deq),
   State#state{adapt_interval = NewAdaptInt}.


publish({Topic, Msg}, #state{retained = Ret, qos = Qos, client = C})
   when is_binary(Msg); is_list(Msg) ->
%%   lager:notice("publish ~s on topic ~p ~n",[Msg, Topic]),
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
do_connect(#state{host = Host, port = Port, client_id = ClientId} = State) ->
   reconnect_watcher:bump(),
   connection_registry:connecting(),
   Opts0 = [{host, Host}, {port, Port}, {keepalive, 30}, {client_id, ClientId}],
   Opts1 = opts_auth(State, Opts0),
   Opts = opts_ssl(State, Opts1),
   lager:info("connect to mqtt broker with: ~p",[Opts]),
   {ok, _Client} = emqttc:start_link(Opts),
   State.

opts_auth(#state{user = <<>>}, Opts) -> Opts;
opts_auth(#state{user = undefined}, Opts) -> Opts;
opts_auth(#state{user = User, pass = Pass}, Opts) -> [{username, User},{password, Pass}] ++ Opts.

opts_ssl(#state{ssl = false}, Opts) -> Opts;
opts_ssl(#state{ssl = true, ssl_opts = SslOpts}, Opts) -> [{ssl, SslOpts}]++ Opts.
