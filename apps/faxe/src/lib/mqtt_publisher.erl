%%%-------------------------------------------------------------------
%%% @author heyoka
%%% @copyright (C) 2019, <COMPANY>
%%% @doc
%%% this server consumes from a persistent esq queue and publishes to a mqtt broker
%%% @end
%%% Created : 26. Jul 2019 11:41
%%%-------------------------------------------------------------------
-module(mqtt_publisher).
-author("heyoka").

-behaviour(gen_server).

%% API
-export([start_link/2]).

%% gen_server callbacks
-export([init/1,
   handle_call/3,
   handle_cast/2,
   handle_info/2,
   terminate/2,
   code_change/3]).

-define(SERVER, ?MODULE).
-define(DEFAULT_QUEUE_FILE, "/tmp/mqtt_q").

-record(state, {
   client,
   connected = false,
   host,
   port,
   qos,
   topic,
   retained = false,
   ssl = false,
   queue_file = ?DEFAULT_QUEUE_FILE,
   queue,
   deq_interval = 5
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
init([#{host := Host, port := Port, topic := Topic, retained := Retained, ssl := UseSSL, qos := Qos}, Queue]) ->
   SslOpts =
   case ssl of
      true ->
         [{keyfile, "certs/tgw_wildcard.key"},
            {certfile, "certs/tgw_wildcard.pem"},
            {cacertfile, "certs/tgw_wildcard.crt"}];
      false -> []
   end,
   {ok, Client} = emqtt:start_link([{host, Host}, {port, Port}, {ssl_opts, SslOpts} ]),
   {ok, _} = emqtt:connect(Client),
   {ok,
      #state{host = Host, port = Port, topic = Topic,
         retained = Retained, ssl = UseSSL, qos = Qos, queue = Queue}}.

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
handle_info({mqttc, C, connected}, State=#state{}) ->
   lager:debug("mqtt client connected!!"),
   NewState = State#state{client = C, connected = true},
   next(NewState),
   {noreply, NewState};
handle_info({mqttc, _C,  disconnected}, State=#state{}) ->
   lager:debug("mqtt client disconnected!!"),
   {noreply, State#state{connected = false, client = undefined}};
handle_info(deq, State=#state{}) ->
   next(State),
   {noreply, State};
handle_info(E, S) ->
   lager:info("unexpected: ~p~n", [E]),
   {noreply, S}.

next(State=#state{queue = Q, deq_interval = Interval}) ->
   case esq:deq(Q) of
      [] -> ok; %lager:info("Queue is empty!"), ok;
      [#{payload := Payload}] -> lager:notice("msg from Q: ~p", [Payload]), publish(Payload, State)
   end,
   erlang:send_after(Interval, self(), deq).

publish(Msg, #state{retained = Ret, qos = Qos, client = C, topic = Topic}) when is_binary(Msg) ->
   lager:notice("publish ~p on topic ~p ~n",[Msg, Topic]),
   {ok, PacketId} = emqtt:publish(C, Topic, Msg, [{qos, Qos}, {retained, Ret}]),
   lager:notice("sent msg and got PacketId: ~p",[PacketId]).

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
