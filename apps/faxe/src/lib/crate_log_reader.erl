%%%-------------------------------------------------------------------
%%% @author heyoka
%%% @copyright (C) 2020, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 25. Feb 2020 10:10
%%%-------------------------------------------------------------------
-module(crate_log_reader).
-author("heyoka").

-behaviour(gen_server).

%% API
-export([start_link/0, read_logs/1, read_logs/2]).

%% gen_server callbacks
-export([init/1,
   handle_call/3,
   handle_cast/2,
   handle_info/2,
   terminate/2,
   code_change/3]).

-define(SERVER, ?MODULE).
-define(DB_OPTIONS, #{
   codecs => [{faxe_epgsql_codec, nil}, {epgsql_codec_json, {jiffy, [], [return_maps]}}],
   timeout => 3000
}).
-define(TABLE, <<"lager_test">>).

-record(state, {
   host,
   port,
   user,
   pass,
   client,
   db_opts,
   table
}).

%%%===================================================================
%%% API
%%%===================================================================
read_logs(Flow) ->
   read_logs(Flow, <<"warning">>).
read_logs(Flow, Severity) ->
   gen_server:call(?SERVER, {read, Flow, Severity}).

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
   {ok, Opts0} = application:get_env(faxe, crate),
   Host0 = proplists:get_value(host, Opts0),
   Port = proplists:get_value(port, Opts0),
   User0 = proplists:get_value(user, Opts0),
   Pass0 = proplists:get_value(pass, Opts0, <<>>),
   DB0 = proplists:get_value(database, Opts0, <<"doc">>),
   process_flag(trap_exit, true),
   Host = binary_to_list(Host0),
   User = binary_to_list(User0),
   Pass = binary_to_list(Pass0),
   DB = binary_to_list(DB0),
   Opts = #{host => Host, port => Port, username => User, pass => Pass, database => DB},
   DBOpts = maps:merge(?DB_OPTIONS, Opts),
   State = #state{
      host = Host,
      port = Port,
      user = User,
      pass = Pass,
      db_opts = DBOpts,
      table = ?TABLE
   },
   NewState = connect(State),
   {ok, NewState}.

%%--------------------------------------------------------------------
handle_call({read, Flow, Severity}, _From, State = #state{client = C, table = Table}) ->
   Query = build_query(Table, Flow, Severity),
   lager:notice("Query is: ~p",[Query]),
   Response = epgsql:squery(C, Query),
   Result = handle_result(Response),
   lager:notice("Result is: ~p", [Result]),
   {reply, Result, State};
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

handle_info({'EXIT', _C, Reason}, State = #state{}) ->
   lager:notice("EXIT epgsql with Reason: ~p", [Reason]),
   NewState = connect(State),
   {ok, NewState};
handle_info(What, State) ->
   lager:warning("++other info : ~p",[What]),
   {ok, State}.

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
connect(State = #state{db_opts = Opts}) ->
   lager:warning("db opts: ~p",[Opts]),
   {ok, C} = epgsql:connect(Opts),
   State#state{client = C}.

build_query(Table, Flow, _Severity) ->
   Q0 = ["SELECT DATE_FORMAT(ts) as datetime,severity,flow,comp,message,meta FROM doc.", Table, " WHERE flow = ",
      " '", Flow, "' ORDER BY ts DESC LIMIT 20;"],
   iolist_to_binary(Q0).

handle_result({ok, Columns, Rows}) ->
   CNames = columns(Columns, []),
   {ok, rows_to_list(CNames, Rows, [])};
handle_result(Other) ->
   Other.

columns([], ColumnNames) ->
   lists:reverse(ColumnNames);
columns([{column, Name, _Type, _, _, _, _}|RestC], ColumnNames) ->
   columns(RestC, [Name|ColumnNames]).

rows_to_list(_, [], RowList) ->
   lists:reverse(RowList);
rows_to_list(Columns, [Row|Rows], Acc) ->
   rows_to_list(Columns, Rows, [row_to_map(Columns, Row)|Acc]).

row_to_map(Columns, Row) ->
   maps:from_list(lists:zip(Columns, tuple_to_list(Row))).