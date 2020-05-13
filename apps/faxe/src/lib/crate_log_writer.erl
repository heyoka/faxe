%%%-------------------------------------------------------------------
%%% @author heyoka
%%% @copyright (C) 2020, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 24. Feb 2020 14:39
%%%-------------------------------------------------------------------
-module(crate_log_writer).
-author("heyoka").

-behaviour(gen_server).

%% API
-export([start_link/0]).

%% gen_server callbacks
-export([init/1,
   handle_call/3,
   handle_cast/2,
   handle_info/2,
   terminate/2,
   code_change/3]).

-define(SERVER, ?MODULE).
-define(MAX_TIME, 5000).
-define(MAX_COUNT, 10).
%%% db
-define(KEY, <<"stmt">>).
-define(PATH, <<"/_sql">>).
-define(ARGS, <<"bulk_args">>).
-define(DEFAULT_SCHEMA_HDR, <<"Default-Schema">>).
-define(QUERY_TIMEOUT, 5000).

-define(DATABASE, <<"">>).

-record(state, {
   buffer = [],
   count = 1,
   timer_ref = undefined,
   max_time = 5000,
   max_cnt = 10,
   host,
   port,
   fields,
   client,
   stmt
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
   {ok, Opts0} = application:get_env(lager, handlers),
   Opts = proplists:get_value(lager_flowlog_backend, Opts0),
   Host0 = proplists:get_value(host, Opts),
   Port = proplists:get_value(port, Opts),
   Fields = proplists:get_value(fields, Opts, []),
   Host = binary_to_list(Host0),
   erlang:send_after(0, self(), start_client),
   Q = build_stmt(<<"doc.lager_test">>, Fields),
   catch (lager_flowlog_backend ! writer_ready),
   State = #state{
      host = Host,
      port = Port,
      stmt = Q,
      max_time = ?MAX_TIME,
      max_cnt = ?MAX_COUNT
   },
   {ok, start_timeout(State)}.


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
-spec(handle_info(Info :: timeout() | term(), State :: #state{}) ->
   {noreply, NewState :: #state{}} |
   {noreply, NewState :: #state{}, timeout() | hibernate} |
   {stop, Reason :: term(), NewState :: #state{}}).
handle_info(send, State = #state{buffer = []}) ->
   {noreply, start_timeout(State)};
handle_info(send, State = #state{buffer = Rows, client = Client, stmt = Stmt}) ->
   do_send(Client, Stmt, Rows),
   {noreply, start_timeout(State#state{count = 1, buffer = []})};
handle_info({log, Row}, State = #state{buffer = Rows, count = Cnt, max_cnt = Cnt,
      stmt = Stmt, client = Client}) ->
   cancel_timeout(State),
   do_send(Client, Stmt, [Row|Rows]),
   NewState = State#state{count = 1, buffer = []},
   {noreply, start_timeout(NewState)};
handle_info({log, Row}, State = #state{buffer = Rows, count = Cnt}) ->
   % add row to buffer
   {noreply, State#state{buffer = [Row|Rows], count = Cnt+1}};
handle_info({'DOWN', _MonitorRef, _Type, Pid, _Info}, State = #state{client = Pid}) ->
   lager:warning("gun is down"),
   handle_info(start_client, State);
handle_info(start_client, State) ->
   NewState = start_client(State),
   {noreply, NewState};
handle_info(_Req, State) ->
   {noreply, State}.

start_client(State = #state{host = Host, port = Port}) ->
   Opts = #{connect_timeout => 3000},
   case gun:open(Host, Port, Opts) of
      {ok, C} ->
         erlang:monitor(process, C),
         case gun:await_up(C) of
            {ok, _} ->
               State#state{client = C};
            _ ->
               lager:warning("timeout connecting to ~p:~p", [Host, Port]),
               recon(State)
         end;
      {error, Err} ->
         lager:warning("error connecting to ~p:~p ~p", [Host, Port, Err]),
         recon(State)
   end.

recon(State) ->
   erlang:send_after(1000, self(), start_client),
   State#state{client = undefined}.

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
terminate(_, #state{client = Client}) ->
   gun:close(Client).

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
build_stmt(Table, ValueList) ->
   Q0 = <<"INSERT INTO ", Table/binary>>,
   Fields = iolist_to_binary(lists:join(<<", ">>, ValueList)),
   Q1 = <<Q0/binary, " (", Fields/binary, ") VALUES ">>,
   QMarks = iolist_to_binary(lists:join(<<", ">>, lists:duplicate(length(ValueList), "?"))),
   Q = <<Q1/binary, "(", QMarks/binary, ")">>,
   Q.

do_send(Client, Stmt, Rows) ->
   Body = jiffy:encode(#{?KEY => Stmt, ?ARGS => Rows}),
   Headers = [{?DEFAULT_SCHEMA_HDR, ?DATABASE}, {<<"content-type">>, <<"application/json">>}],
   _Ref = gun:post(Client, ?PATH, Headers, Body),
   gun:flush(Client).


cancel_timeout(State = #state{timer_ref = Timer}) ->
   catch erlang:cancel_timer(Timer),
   State#state{timer_ref = undefined}.

start_timeout(State = #state{max_time = MTime}) ->
   TRef = timer:send_after(MTime, self(), send),
   State#state{timer_ref = TRef}.