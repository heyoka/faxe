%% Date: 31.03.23 - 09:40
%% execute a one shot statement against a postgreSQL compatible db, (PostGre, Crate, ...)
%% Ⓒ 2023 heyoka
%%
-module(esp_postgre_statement).
-author("Alexander Minichmair").

-include("faxe.hrl").
-include("faxe_epgsql_response.hrl").

-behavior(df_component).
%% API
-export([init/3, process/3, options/0, handle_info/2, check_options/0, shutdown/1]).

-record(state, {
   host        :: string(),
   port        :: non_neg_integer(),
   user        :: string(),
   pass        :: string(),
   client,
   statement   :: iodata(),
   retried = 0 :: non_neg_integer(),
   retries = 2 :: non_neg_integer(),
   db_opts     :: map(),
   fn_id
}).

-define(DB_OPTIONS, #{
   codecs => [{faxe_epgsql_codec, nil}, {epgsql_codec_json, {jiffy, [], [return_maps]}}],
   timeout => 5000
}).


options() ->
   [
      {host, string, {crate, host}},
      {port, integer, {crate, port}},
      {user, string, {crate, user}},
      {pass, string, <<>>},
      {statement, string},
      {retries, integer, 2}
   ].

check_options() ->
   [
%%      {one_of, result_type, [<<"batch">>, <<"point">>]},
%%      {func, statement, fun faxe_util:check_select_statement/1, <<"seems not to be a valid sql select statement">>}
   ].


init(NodeId, _Inputs, #{host := Host0, port := Port, user := User, pass := Pass,
   statement := Q0, retries := Retries}) ->

   process_flag(trap_exit, true),
   Host = binary_to_list(Host0),
   Opts = #{host => Host, port => Port, username => User, password => Pass},
   DBOpts = maps:merge(?DB_OPTIONS, Opts),

   State = #state{
      host = Host, port = Port, user = User, pass = Pass,
      statement = Q0, retries = max(1, Retries),
      db_opts = DBOpts, fn_id = NodeId},
   connection_registry:reg(NodeId, Host, Port, <<"pgsql">>),
   erlang:send_after(0, self(), reconnect),
   {ok, all, State}.

process(_In, _P = #data_point{}, State = #state{}) ->
   {ok, State};
process(_In, _B = #data_batch{}, State = #state{}) ->
   {ok, State}.

handle_info(execute_statement, State = #state{retries = Tries, retried = Tries}) ->
   %% reached max retries
   NewState = close(State),
   lager:warning("Could not execute statement with ~p retries!",[Tries]),
   {ok, NewState};
handle_info(execute_statement, State = #state{retried = Tried}) ->
   NewState = execute(State#state{retried = Tried + 1}),
   {ok, NewState};
handle_info({'EXIT', _C, normal}, State = #state{}) ->
   {ok, State#state{client = undefine}};
handle_info({'EXIT', _C, Reason}, State = #state{}) ->
   lager:warning("EXIT epgsql with reason: ~p",[Reason]),
   NewState = connect(State),
   {ok, NewState#state{}};
handle_info(reconnect, State) ->
   {ok, connect(State)};
handle_info(What, State) ->
   lager:warning("++other info : ~p",[What]),
   {ok, State}.

shutdown(State = #state{client = _C}) ->
   close(State).

connect(State = #state{db_opts = Opts}) ->
   connection_registry:connecting(),
%%   lager:info("db opts: ~p",[Opts]),
   case epgsql:connect(Opts) of
      {ok, C} ->
         connection_registry:connected(),
         erlang:send_after(0, self(), execute_statement),
         State#state{client = C};
      {error, What} ->
         lager:warning("Error connecting to crate: ~p",[What]),
         State
   end.

execute(State = #state{}) ->
   case do_execute(State) of
      true ->
         %% done emit empty data-point
         dataflow:emit(flowdata:new()),
         %% close connection, we are done here
         close(State);
      false ->
         erlang:send_after(500, self(), execute_statement),
         State
   end.

do_execute(_State = #state{client = C, statement = Statement}) ->
   case epgsql:squery(C, Statement) of
      {ok, Count , Cols , Rows } ->
         lager:notice("ok executing statement: Count ~p, Cols ~p, Rows ~p",[Count, Cols, Rows]),
         true;
      {ok, Cols , Rows } ->
         lager:notice("ok executing statement: Cols ~p, Rows ~p",[Cols, Rows]),
         true;
      {ok, Count} ->
         lager:notice("ok executing statement: Count: ~p",[Count]),
         true;
      ok ->
         lager:notice("ok executing statement"),
         true;
      {error, {error,error,<<"XX000">>,internal_error, ErrorMessage, _Trace}} ->
         lager:error("Error executing Statement: ~p",[ErrorMessage]),
         false;
      {error, What} ->
         lager:error("Error executing Statement: ~p",[What]),
         false
   end.

close(State = #state{client = C}) ->
   catch epgsql:close(C),
   connection_registry:disconnected(),
   State#state{client = undefined}.