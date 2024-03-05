%%%-------------------------------------------------------------------
%%% @author heyoka
%%% @copyright (C) 2023, <COMPANY>
%%% @doc
%%% @end
%%%-------------------------------------------------------------------
-module(faxe_epgsql_stmt).

-behaviour(gen_server).

-export([start_link/1, execute/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
  code_change/3]).

-record(state, {
  client            :: pid(),
  db_opts           :: map(),
  host              :: string(),
  port              :: non_neg_integer(),
  user              :: string(),
  pass              :: string(),
  parent            :: pid()
}).

-define(DB_OPTIONS, #{
  codecs => [{faxe_epgsql_codec, nil}, {epgsql_codec_json, {jiffy, [], [return_maps]}}],
  timeout => 5000, port => 5432
}).

%%%===================================================================
%%% Spawning and gen_server implementation
%%%===================================================================

execute(Server, Statement) ->
  gen_server:call(Server, {stmt, Statement}).

start_link(Opts) ->
  gen_server:start_link(?MODULE, [{parent, self()}, {opts, Opts}], []).

init([{parent, Parent}, {opts, #{host := Host0, port := Port, username := User, password := Pass, database := _Db} = Opts}]) ->
  process_flag(trap_exit, true),
%%  Host = binary_to_list(Host0),
%%  Opts = #{host => Host0, port => Port, username => User, password => Pass, database => Db},
  DBOpts0 =
  case maps:get(tls, Opts, false) of
    true -> ?DB_OPTIONS#{ssl => true};
    _Else -> ?DB_OPTIONS
  end,
  DBOpts = maps:merge(DBOpts0, Opts),
  lager:notice("[~p] DBOPTS: ~p",[?MODULE, DBOpts]),
  erlang:send_after(0, self(), reconnect),
  {ok, #state{host = Host0, port = Port, user = User, pass = Pass, db_opts = DBOpts, parent = Parent}}.

handle_call({stmt, Statement}, _From, State = #state{client = C}) ->
  Result = epgsql:squery(C, Statement),
  {reply, Result, State};
handle_call(_Request, _From, State = #state{}) ->
  {reply, ok, State}.

handle_cast(_Request, State = #state{}) ->
  {noreply, State}.

handle_info({'EXIT', C, normal}, State = #state{client = C}) ->
  {noreply, State#state{client = undefined}};
handle_info({'EXIT', C, Reason}, State = #state{client = C, parent = Parent}) ->
  lager:warning("EXIT epgsql with reason: ~p",[Reason]),
  Parent ! {?MODULE, disconnected},
  NewState = connect(State),
  {noreply, NewState#state{}};
handle_info(reconnect, State) ->
  {noreply, connect(State)};
handle_info(_Info, State = #state{}) ->
  {noreply, State}.

terminate(_Reason, _State = #state{client = C}) ->
  catch epgsql:close(C).

code_change(_OldVsn, State = #state{}, _Extra) ->
  {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
connect(State = #state{db_opts = Opts, parent = Parent}) ->
%%  lager:notice("db opts ~p",[Opts]),
  case epgsql:connect(Opts) of
    {ok, C} ->
      Parent ! {?MODULE, connected},
      State#state{client = C};
    {error, What} ->
      lager:warning("Error connecting postgre: ~p",[What]),
      State
  end.