%% Date: 30.12.16 - 23:01
%% CrateDB Writer that uses crate's http endpoint
%% Ⓒ 2019 heyoka
%%
-module(esp_crate_out).
-author("Alexander Minichmair").

-include("faxe.hrl").

-behavior(df_component).
%% API
-export([init/3, process/3, options/0, handle_info/2, do_send/5, shutdown/1, metrics/0,
   check_options/0, quote_identifier/1, check_table_identifier/1, check_column_identifier/1, is_idle/1]).

-record(state, {
   host :: string(),
   port :: non_neg_integer(),
   path :: string(),
   user :: binary(),
   pass :: binary(),
   key :: string(),
   client,
   database,
   table,
   table_field,
   query,
   query_from_lambda = false :: true|false,
   %% the query point is used when the query is built with a lambda and also to get a decent error message
   %% from CRATE when batch INSERT just returns a -2 for a row, but no error
   query_point :: undefined|#data_point{},
   db_fields,
   faxe_fields,
   remaining_fields_as,
   failed_retries,
   tries,
   tls,
   fn_id,
   last_error,
   debug_mode = false,
   flow_inputs,
   ignore_resp_timeout,
   error_trace = false,
   pg_client :: pid()
}).

-define(QUOTEABLE, [
   <<"0">>, <<"1">>, <<"2">>, <<"3">>, <<"4">>, <<"5">>, <<"6">>, <<"7">>, <<"8">>, <<"9">>]).

-define(KEY, <<"stmt">>).
-define(PATH, <<"/_sql">>).
-define(ACTIVE_ERROR_TRACE, <<"?error_trace=true">>).
-define(ARGS, <<"bulk_args">>).
-define(DEFAULT_SCHEMA_HDR, <<"Default-Schema">>).
-define(AUTH_HEADER_KEY, <<"Authorization">>).
-define(QUERY_TIMEOUT, 15000).
-define(FAILED_RETRIES, 3).

-define(CONNECT_OPTS, #{connect_timeout => 3000}).


options() ->
   [
      {host, string, {crate_http, host}},
      {port, integer, {crate_http, port}},
      {tls, is_set, {crate_http, tls, enable}},
      {table, any},
      {user, string, {crate_http, user}},
      {pass, string, {crate_http, pass}},
      {database, string, <<"doc">>},
      {db_fields, list, undefined},
      {faxe_fields, string_list, undefined},
      {remaining_fields_as, string, undefined},
      {max_retries, integer, ?FAILED_RETRIES},
      {error_trace, boolean, false},
      {ignore_response_timeout, boolean, true}
   ].

check_options() ->
   [
      {func, table, fun(E) -> is_function(E) orelse is_binary(E) end,
         <<" must be either a string or a lambda function">>},
      {func, db_fields,
         fun
            (undefined) -> true;
            (Fields) ->
               lists:all(fun(E) -> is_function(E) orelse is_binary(E) end, Fields)
            end,
            <<" list may only contain strings and lambda functions">>}


   ].

metrics() ->
   [
      {?METRIC_BYTES_SENT, meter, []}
   ].

init(NodeId, Inputs,
    #{host := Host0, port := Port, database := DB, table := Table, user := User, pass := Pass,
       tls := Tls, db_fields := DBFields0, faxe_fields := FaxeFields, error_trace := ETrace,
       remaining_fields_as := RemFieldsAs, max_retries := MaxRetries,
       ignore_response_timeout := IgnoreRespTimeout}) ->

   Host = binary_to_list(Host0),
   erlang:send_after(0, self(), query_init),
%%   Query = maybe_build_query(DBFields, Table, RemFieldsAs),
   connection_registry:reg(NodeId, Host, Port, <<"http">>),
   %% use fully qualified table name here, ie. doc."0x23d"
   Path = case ETrace of false -> ?PATH; true -> <<?PATH/binary, ?ACTIVE_ERROR_TRACE/binary>> end,

   DBFields =
   case DBFields0 of
      undefined -> undefined;
      _ when is_list(DBFields0) -> [quote_identifier(DBField) || DBField <- DBFields0]
   end,
   State = #state{
      host = Host, port = Port,
      database = quote_identifier(DB),
      user = User, pass = Pass,
      failed_retries = MaxRetries,
      remaining_fields_as = RemFieldsAs,
      tls = Tls, path = Path,
      table = quote_identifier(Table),
      db_fields = DBFields,
      faxe_fields = FaxeFields,
      error_trace = ETrace,
      fn_id = NodeId, flow_inputs = Inputs, ignore_resp_timeout = IgnoreRespTimeout},
%%   NewState = query_init(State),
%%   lager:warning("QUERY INIT Schema:~p ~p",[DB, {NewState#state.query, NewState#state.query_from_lambda}]),
   {ok, all,State}.

%%% DATA IN
%% empyt batch -> continue
process(_In, #data_batch{points = []}, State = #state{}) ->
   {ok, State};
%% not connected -> drop message
%% @todo buffer these messages when not connected
process(_In, _DataItem, State = #state{client = undefined}) ->
   lager:notice("got item when not connected"),
   {ok, State};
%% we do not have a prepare query yet
process(_In, DataItem, State = #state{query_from_lambda = true,
      table = Table, db_fields = DbFields, database = DB, remaining_fields_as = RemFields}) ->
   Point = get_query_point(DataItem),
   Query = build_query(DbFields, DB, Table, RemFields, Point),
   do_process(DataItem, State#state{query = Query, query_point = Point});
process(_In, DataItem, State) ->
   do_process(DataItem, State#state{query_point = get_query_point(DataItem)}).

%% idle stop feature
is_idle(State) ->
   {false, State}.

do_process(DataItem, State = #state{fn_id = _FNId}) ->
   _NewState = send(DataItem, State),
%%   dataflow:maybe_debug(item_in, 1, DataItem, FNId, State#state.debug_mode),
   {ok, State}.

handle_info({'DOWN', _MonitorRef, _Type, Pid, _Info}, State = #state{client = Pid}) ->
   connection_registry:disconnected(),
   lager:warning("gun is down"),
   handle_info(start_client, State#state{client = undefined});

handle_info(query_init, State=#state{faxe_fields = undefined, db_fields = undefined}) ->
   %% special case to retrieve column names automatically
   NewState =
   case get_fields(State) of
      {ok, NewState1} ->
         erlang:send_after(0, self(), query_init),
         NewState1;
      {_, NewState2} -> NewState2
   end,
   {ok, NewState};
handle_info(query_init, State) ->
   NewState = query_init(State),
%%   lager:warning("after query init ~p",[lager:pr(NewState, ?MODULE)]),
   erlang:send_after(0, self(), start_client),
   {ok, NewState};
handle_info(start_client, State) ->
   NewState = start_client(State),
   {ok, NewState};
handle_info(start_debug, State) -> {ok, State#state{debug_mode = true}};
handle_info(stop_debug, State) -> {ok, State#state{debug_mode = false}};
handle_info(_Req, State) ->
   {ok, State}.

shutdown(#state{client = C}) ->
   connection_registry:disconnected(),
   gun:close(C).

-spec start_client(#state{}) -> #state{}.
start_client(State = #state{host = Host, port = Port, tls = Tls}) ->
   connection_registry:connecting(),
   Options =
      case Tls of
         true -> ?CONNECT_OPTS#{transport => tls};
         false -> ?CONNECT_OPTS
      end,
   case gun:open(Host, Port, Options) of
      {ok, C} ->
         erlang:monitor(process, C),
         case gun:await_up(C) of
            {ok, _} ->
               connection_registry:connected(),
               State#state{client = C};
            {error, What} ->
               lager:warning("error connecting to ~p:~p - ~p", [Host, Port, What]),
               recon(State)
         end;
      {error, Err} ->
         lager:warning("error connecting to ~p:~p ~p", [Host, Port, Err]),
         recon(State)
   end.

recon(State) ->
   erlang:send_after(1000, self(), start_client),
   State#state{client = undefined}.

get_query_point(#data_batch{points = [P|_]}) ->
   P;
get_query_point(#data_point{} = P) ->
   P.

%%% DATA OUT
send(Item, State = #state{query = Q, faxe_fields = Fields, remaining_fields_as = RemFieldsAs,
      user = User, pass = Pass}) ->
   Query = build(Item, Q, Fields, RemFieldsAs),
   Headers = [{<<"content-type">>, <<"application/json">>}] ++ http_lib:basic_auth_header(User, Pass),
   NewState = do_send(Item, Query, Headers, 0, State#state{last_error = undefined}),
   MBytes = faxe_util:bytes(Query),
   node_metrics:metric(?METRIC_BYTES_SENT, MBytes, State#state.fn_id),
   node_metrics:metric(?METRIC_ITEMS_OUT, 1, State#state.fn_id),
   NewState.

resend_single(State = #state{query = Q, faxe_fields = Fields, remaining_fields_as = RemFieldsAs,
   user = User, pass = Pass, query_point = Item}) ->
   Query = build(Item, Q, Fields, RemFieldsAs),
   Headers = [{<<"content-type">>, <<"application/json">>}] ++ http_lib:basic_auth_header(User, Pass),
   do_send(Item, Query, Headers, State#state.failed_retries-1, State#state{last_error = undefined}).


%% bind values to the statement
-spec build(#data_point{}|#data_batch{}, binary(), list(), binary()) -> iodata().
build(Item, Query, Fields, RemFieldsAs) ->
   BulkArgs0 = build_value_stmt(Item, Fields, RemFieldsAs),
   BulkArgs =
      case Item of
         #data_point{} -> [BulkArgs0];
         _ -> BulkArgs0
      end,
   jiffy:encode(#{?KEY => Query, ?ARGS => BulkArgs}).

do_send(_Item, _Body, _Headers, MaxFailedRetries, S = #state{failed_retries = MaxFailedRetries, last_error = Err}) ->
   lager:warning("could not send ~p with ~p retries, last error: ~p", [_Body, MaxFailedRetries, Err]),
   S#state{last_error = undefined};
do_send(Item, Body, Headers, Retries, State = #state{client = Client, fn_id = FNId, path = Path}) ->
   Ref = gun:post(Client, Path, Headers, Body),
   case catch(get_response(Client, Ref, State#state.ignore_resp_timeout)) of
      ok ->
         dataflow:ack(Item, State#state.flow_inputs),
         dataflow:maybe_debug(item_out, 1, Item, FNId, State#state.debug_mode),
         State;
      {error, retry_single} ->
         %% try with single data-point and done
         resend_single(State);
      {error, What} ->
%%         lager:warning("could not send ~p: error in request: ~p", [Body, What]),
         do_send(Item, Body, Headers, Retries+1, State#state{last_error = What});

      O ->
         lager:warning("sending gun post: ~p",[O]),
         do_send(Item, Body, Headers, Retries+1, State#state{last_error = O})
   end.


build_value_stmt(_B = #data_batch{points = Points}, Fields, RemFieldsAs) ->
   build_batch(Points, Fields, RemFieldsAs, []);
build_value_stmt(P = #data_point{ts = Ts}, Fields, undefined) ->
   DataList0 = [Ts| flowdata:fields(P, Fields, null)],
   DataList0;
build_value_stmt(P = #data_point{}, Fields, _RemFieldsAs) ->
   DataList = build_value_stmt(P, Fields, undefined),
   Rem = flowdata:to_map_except(P, [<<"ts">>|Fields]),
   DataList ++ [Rem].

build_batch([], _FieldList, _RemFieldsAs, Acc) ->
   Acc;
build_batch([Point|Points], FieldList, RemFieldsAs, Acc) ->
   NewAcc = [build_value_stmt(Point, FieldList, RemFieldsAs) | Acc],
   build_batch(Points, FieldList, RemFieldsAs, NewAcc).

%% build base query
query_init(State = #state{table = DbTable}) when is_function(DbTable) ->
   State#state{query_from_lambda = true};
query_init(State = #state{table = Table, db_fields = DbFields, database = DB, remaining_fields_as = RemF}) ->
   FromLambda = lists:any(fun(F) -> is_function(F) end, DbFields),
   case FromLambda of
      false ->
         TableName = <<DB/binary, ".", Table/binary>>,
         Query = build_query(DbFields, TableName, RemF),
         State#state{query = Query, query_from_lambda = false};
      true ->
         State#state{query_from_lambda = FromLambda}
   end.


%% build the query with lambda funs
build_query(DbFields0, DB, Table0, RemFieldsAs, P=#data_point{}) when is_list(DbFields0) ->
   Table =
   case is_function(Table0) of
      true ->
         TName = faxe_lambda:execute(P, Table0),
         <<DB/binary, ".", TName/binary>>;
      false -> Table0
   end,
   FieldsFun =
   fun(E) ->
      case is_function(E) of
         true -> faxe_lambda:execute(P, E);
         false -> E
      end
   end,
   DbFields = lists:map(FieldsFun, DbFields0),
   build_query(DbFields, Table, RemFieldsAs).


build_query(ValueList0, Table, RemFieldsAs) when is_list(ValueList0) ->
   Q0 = <<"INSERT INTO ", Table/binary>>,
   ValueList1 = [<<"ts">>|ValueList0],
   ValueList =
   case RemFieldsAs of
      undefined -> ValueList1;
      AsName -> ValueList1 ++ [AsName]
   end,
   Fields = iolist_to_binary(lists:join(<<", ">>, ValueList)),
   Q1 = <<Q0/binary, " (", Fields/binary, ") VALUES ">>,
   QMarks = iolist_to_binary(lists:join(<<", ">>, lists:duplicate(length(ValueList), "?"))),
   Q = <<Q1/binary, "(", QMarks/binary, ")">>,
   Q.

get_response(Client, Ref, Ignore) ->
   case gun:await(Client, Ref, ?QUERY_TIMEOUT) of
      {response, _IsFin, Status, _Headers} ->
%%   lager:info("response Status: ~p, Headers: ~p" ,[Status, _Headers]),
         {ok, Message} = gun:await_body(Client, Ref),
%%   handle_response_message(Message),
         handle_response(integer_to_binary(Status), Message);
      {error, timeout} ->
         case Ignore of
            true ->
               lager:notice("response timeout ! (~p)", [?QUERY_TIMEOUT]),
               ok;
            false -> {error, timeout}
         end;
      Other ->
         {error, Other}
   end.

-spec handle_response(integer(), binary()) -> ok|{error, invalid}|{failed, term()}.
handle_response(<<"200">>, BodyJSON) ->
   handle_response_message(BodyJSON);
handle_response(<<"4", _/binary>> = S,_BodyJSON) ->
   lager:error("Error ~p: ~p",[S, _BodyJSON]),
   {error, invalid};
handle_response(<<"503">>, _BodyJSON) ->
   {failed, not_available};
handle_response(<<"5", _/binary>> = S,_BodyJSON) ->
   lager:error("Error ~p with body ~p",[S, _BodyJSON]),
   {failed, server_error};
handle_response({error, What}, {error, Reason}) ->
   lager:error("Other err: ~p",[{What, Reason}]),
   {failed, {What, Reason}}.

handle_response_message(RespMessage) ->
   Response = jiffy:decode(RespMessage, [return_maps]),
%%   lager:info("res ~p",[Response]),
   case Response of
      #{<<"results">> := Results} ->
         %% count where rows := -2
         NotWritten = lists:foldl(
            fun
               (#{<<"rowcount">> := -2, <<"error_message">> := E}, {Count, Errors}) -> {Count+1, Errors++[E]};
               (#{<<"rowcount">> := -2}, {Count, Errors}) -> {Count+1, Errors};
               (_, Acc) -> Acc
            end, {0, []}, Results),
         case NotWritten of
            {0, []} -> ok;
            {C, Errs} ->
               lager:error("CrateDB: ~p of ~p rows not written, errors: ~p, will resend single",[C, length(Results), Errs]),
               {error, retry_single}
         end
      end.



get_fields(State = #state{table = Tab0, database = Db0}) ->
   Tab = binary:replace(Tab0, <<"\"">>, <<>>, [global]),
   Db = binary:replace(Db0, <<"\"">>, <<>>, [global]),
   Stmt = [
      <<"SELECT column_name FROM information_schema.columns WHERE table_schema = '">>,
         <<Db/binary>>,<<"' AND table_name = '">>, <<Tab/binary>>,
      <<"' AND column_name NOT IN ('ts', 'ts_partition') AND column_details['path'] = []">>],
   {PgClient, NewState} = get_pg_client(State),
   receive
      {faxe_epgsql_stmt, connected} ->
         Res = faxe_epgsql_stmt:execute(PgClient, Stmt),
%%         lager:info("Result from column statement: ~p",[Res]),
         case Res of
            {ok,[{column,<<"column_name">>,varchar,_,_,_,_,_,_}], Columns} ->
               ColumnNames = lists:map(fun({ColName}) -> ColName end, Columns),
               DbFields = [quote_identifier(DbF) || DbF <- ColumnNames],
               gen_server:stop(PgClient),
               {ok, NewState#state{faxe_fields = ColumnNames, db_fields = DbFields, pg_client = undefined}};
            O ->
               lager:warning("cannot get column names: ~p",[O]),
               erlang:send_after(2000, self(), query_init),
               {false, NewState}
         end
   after 4000 ->
      lager:error("error getting column names from table"),
      erlang:send_after(2000, self(), query_init),
      {false, NewState}
   end.

get_pg_client(State = #state{pg_client = C}) when is_pid(C) ->
   {C, State};
get_pg_client(State = #state{pg_client = undefined,
   host = Host, port = Port, user = User, pass = Pass, database = Db0}) ->
   Db = binary:replace(Db0, <<"\"">>, <<>>, [global]),
   {ok, PgClient} = faxe_epgsql_stmt:start_link(
      #{host => Host, port => Port,
         username => faxe_util:to_list(User),
         password => faxe_util:to_list(Pass),
         database => Db}
   ),
   {PgClient, State#state{pg_client = PgClient}}.



check_table_identifier(<<"_", _/binary>>) -> false;
check_table_identifier(<<"\"_", _/binary>>) -> false;
check_table_identifier(Ident) ->
   % CRATE DB identifiers
%%   \ / * ? " < > | <whitespace> , # .
   re:run(Ident, <<"[(A-Z)|(\\)|/|\*|\?|<|>|\|\s|,|#|\.]">>) == nomatch.

check_column_identifier(Ident) ->
   binary:match(Ident, [<<"[">>, <<"]">>, <<".">>]) == nomatch.

%% always quote identifiers (table and column names)
quote_identifier(<<"\"", _/binary>> = Ident) ->
   Ident;
quote_identifier(Identifier) when is_binary(Identifier) ->
   <<"\"", Identifier/binary, "\"">>.
