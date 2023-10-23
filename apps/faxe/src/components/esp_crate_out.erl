%% Date: 30.12.16 - 23:01
%% CrateDB Writer that uses crate's http endpoint
%% @todo 2 issues to be resolved:
%% 1. duplicate items
%% 2. when resend_single, do not attempt to retry (in case of {failed, _})
%% Ⓒ 2019 heyoka
%%
-module(esp_crate_out).
-author("Alexander Minichmair").

-include("faxe.hrl").

-behavior(df_component).
%% API
-export([init/3, process/3, options/0, handle_info/2, do_send/4, shutdown/1, metrics/0,
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
   %% from CRATE when batch INSERT just returns a -2 for a row, but no error (message)
   query_point :: undefined|#data_point{},
   db_fields,
   faxe_fields,
   remaining_fields_as,
   failed_retries,
   tries,
   headers :: list(),
   tls,
   fn_id,
   last_error,
   debug_mode = false,
   flow_inputs,
   ignore_resp_timeout,
   error_trace = false,
   pg_client :: pid(),
   buffer = [] :: list(),
   busy = false :: true|false,
   single_resend = false :: true|false,
   use_flow_ack = false :: true|false,

   pending_data = #{} :: map(),
   dedup_queue :: memory_queue:memory_queue()
}).

-define(KEY, <<"stmt">>).
-define(PATH, <<"/_sql">>).
-define(HEADERS, [{<<"content-type">>, <<"application/json">>}]).
-define(ACTIVE_ERROR_TRACE, <<"?error_trace=true">>).
-define(ARGS, <<"bulk_args">>).
-define(DEFAULT_SCHEMA_HDR, <<"Default-Schema">>).
-define(AUTH_HEADER_KEY, <<"Authorization">>).
-define(QUERY_TIMEOUT, faxe_config:get_sub(crate_http, query_timeout)).
-define(FAILED_RETRIES, 3).
-define(FAILED_RETRY_INTERVAL, 1000).

-define(CONNECT_OPTS, #{connect_timeout => faxe_config:get_sub(crate_http, connection_timeout)}).

-define(DEDUP_QUEUE_SIZE, 250).

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
      {ignore_response_timeout, boolean, true},
      {use_flow_ack, boolean, {amqp, flow_ack, enable}}
   ].

check_options() ->
   [
      {func, table, fun(E) -> faxe_lambda:is_lambda(E) orelse is_binary(E) end,
         <<" must be either a string or a lambda function">>},
      {func, db_fields,
         fun
            (undefined) -> true;
            (Fields) ->
               lists:all(fun(E) -> faxe_lambda:is_lambda(E) orelse is_binary(E) end, Fields)
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
       ignore_response_timeout := IgnoreRespTimeout, use_flow_ack := FlowAck}) ->

   lager:notice("~p", [?CONNECT_OPTS]),
   Host = binary_to_list(Host0),
   erlang:send_after(0, self(), query_init),
%%   Query = maybe_build_query(DBFields, Table, RemFieldsAs),
   connection_registry:reg(NodeId, Host, Port, <<"http">>),
   %% use fully qualified table name here, ie. doc."0x23d"
   Path = case ETrace of false -> ?PATH; true -> <<?PATH/binary, ?ACTIVE_ERROR_TRACE/binary>> end,
   Headers = ?HEADERS ++ http_lib:basic_auth_header(User, Pass),

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
      headers = Headers,
      table = quote_identifier(Table),
      db_fields = DBFields,
      faxe_fields = FaxeFields,
      error_trace = ETrace,
      fn_id = NodeId, flow_inputs = Inputs,
      ignore_resp_timeout = IgnoreRespTimeout,
      use_flow_ack = FlowAck,
      dedup_queue = memory_queue:new(?DEDUP_QUEUE_SIZE)},

   {ok, all,State}.

%%% DATA IN
%% empyt batch -> continue
process(_In, #data_batch{points = []}, State = #state{}) ->
   {ok, State};
%% not connected -> buffer
process(_In, DataItem, State = #state{client = undefined, buffer = Buffer}) ->
   lager:notice("got item when not connected"),
   {ok, State#state{buffer = Buffer ++ [DataItem]}};
%% busy -> buffer
process(_In, DataItem, State = #state{busy = true, buffer = Buffer}) ->
   lager:notice("got item when busy"),
   {ok, State#state{buffer = Buffer ++ [DataItem]}};
%% buffer not empty -> buffer
process(_In, DataItem, State = #state{buffer = [_SomeItemm | _] = Buffer}) ->
   lager:notice("got item when buffer not empty"),
   {ok, State#state{buffer = Buffer ++ [DataItem]}};
process(_In, DataItem, State) ->
%%   lager:notice("got item process right away"),
   prepare_process(DataItem, State).

prepare_process(DataItem, State = #state{query_from_lambda = true,
   table = Table, db_fields = DbFields, database = DB, remaining_fields_as = RemFields}) ->
   Point = get_query_point(DataItem),
   Query = build_query(DbFields, DB, Table, RemFields, Point),
   do_process(DataItem, State#state{query = Query, query_point = Point});
prepare_process(DataItem, State) ->
   do_process(DataItem, State#state{query_point = get_query_point(DataItem)}).

%% idle stop feature
is_idle(State) ->
   {false, State}.

do_process(DataItem, State = #state{fn_id = _FNId}) ->
   NewState = send(DataItem, State#state{busy = true}),
%%   dataflow:maybe_debug(item_in, 1, DataItem, FNId, State#state.debug_mode),
   {ok, NewState}.

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
handle_info({send_retry, Item, Body, Retries}, State) ->
   NewState = do_send(Item, Body, Retries, State),
   {ok, NewState};
handle_info(continue, State = #state{buffer = []}) ->
   lager:notice("continue, but buffer is empty"),
   {ok, State};
handle_info(continue, State = #state{buffer = [Item|Rest]}) ->
   lager:notice("continue with item from buffer"),
   prepare_process(Item, State#state{buffer = Rest});
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
               NewState = State#state{client = C},
               maybe_continue(NewState);
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

get_query_point(#data_point{} = P, _Idx) ->
   P;
get_query_point(#data_batch{points = Points}, ListIndex) ->
   lists:nth(ListIndex, Points).

%%% DATA OUT
send(Item, State = #state{query = Q, faxe_fields = Fields, remaining_fields_as = RemFieldsAs}) ->
   {DTag, PHashes, Query} = build(Item, Q, Fields, RemFieldsAs, State),
%%   lager:info("built query ~p",[Query]),
   PendingData = #{last_dtag => DTag, item_hashes => PHashes},
   NewState = do_send(Item, Query, 0, State#state{last_error = undefined, pending_data = PendingData}),
   MBytes = faxe_util:bytes(Query),
   node_metrics:metric(?METRIC_BYTES_SENT, MBytes, State#state.fn_id),
   node_metrics:metric(?METRIC_ITEMS_OUT, 1, State#state.fn_id),
   NewState.

resend_single(Item, State = #state{query = Q, faxe_fields = Fields, remaining_fields_as = RemFieldsAs,
   query_point = QueryItem}) ->
   {_, _, Query} = build(QueryItem, Q, Fields, RemFieldsAs, State),
%%   lager:notice("retry single item ~p",[QueryItem]),
   do_send(Item, Query, State#state.failed_retries-1, State#state{last_error = undefined, single_resend = true}).

%% empty query
do_send(_Item, undefined, _MaxFailedRetries, State) ->
   lager:notice("no query to send, skip ..."),
   done_sending(State);
%% reached max retries
do_send(_Item, _Body, MaxFailedRetries, S = #state{failed_retries = MaxFailedRetries, last_error = Err}) ->
   lager:warning("could not send ~p with ~p retries, last error: ~p", [_Body, MaxFailedRetries, Err]),
   done_sending(S);
do_send(Item, Body, Retries, State = #state{client = Client, path = Path, headers = Headers}) ->
   Ref = gun:post(Client, Path, Headers, Body),
   case catch(get_response(Client, Ref, State#state.ignore_resp_timeout)) of
      ok ->
         done_sending(State);
      {error, retry_single, ListIndex} ->
         %% try with single data-point and done
         QueryPoint = get_query_point(Item, ListIndex),
         resend_single(Item, State#state{query_point = QueryPoint});
      {error, What} ->
         do_send(Item, Body, Retries+1, State#state{last_error = What});
      %% when use_flow_ack retry "endless"
      {failed, Why} when State#state.use_flow_ack == true andalso State#state.single_resend == false ->
         %% we do not increase the retry counter, so basically trying endlessly
         erlang:send_after(?FAILED_RETRY_INTERVAL, self(), {send_retry, Item, Body, Retries}),
         State#state{last_error = Why};
      O ->
         lager:warning("sending gun post: ~p",[O]),
         do_send(Item, Body, Retries+1, State#state{last_error = O})
   end.


done_sending(State = #state{pending_data = #{last_dtag := DTag, item_hashes := HList}, dedup_queue = Queue}) ->
   dataflow:ack(multi, DTag, State#state.flow_inputs),
   NewDedupQ = memory_queue:enq(HList, Queue),
   NewState = State#state{
      last_error = undefined,
      busy = false,
      single_resend = false,
      pending_data = #{},
      dedup_queue = NewDedupQ},
   maybe_continue(NewState).

maybe_continue(State = #state{buffer = []}) ->
   State;
maybe_continue(State = #state{}) ->
   erlang:send_after(0, self(), continue),
   State#state{busy = true}.

%% bind values to the statement
-spec build(#data_point{}|#data_batch{}, binary(), list(), binary(), #state{}) -> {integer(), list, iodata()|undefined}.
build(Item, Query, Fields, RemFieldsAs, #state{dedup_queue = Queue}) ->
   {DTag, PHashes, BulkArgs0}=Res = build_value_stmt(Item, Fields, RemFieldsAs, Queue),
%%   lager:notice("got build result ~p",[Res]),
   JsonQuery =
   case BulkArgs0 of
      [] -> undefined;
      _ ->
         BulkArgs1 = case Item of #data_point{} -> [BulkArgs0]; _ -> BulkArgs0 end,
         jiffy:encode(#{?KEY => Query, ?ARGS => BulkArgs1})
   end,
   {DTag, PHashes, JsonQuery}.

build_value_stmt(_B = #data_batch{points = Points}, Fields, RemFieldsAs, DedupQ) ->
   build_batch(Points, Fields, RemFieldsAs, DedupQ, {0, [], []});
build_value_stmt(P = #data_point{dtag = DTag}, Fields, RemFields, DedupQ) ->
   PHash = erlang:phash2(P#data_point{dtag = undefined}),
   case memory_queue:member(PHash, DedupQ) of
      true -> {DTag, [], []};
      false -> {DTag, [PHash], build_value_stmt2(P, Fields, RemFields)}
   end.

build_value_stmt2(P = #data_point{ts = Ts}, Fields, undefined) ->
   DataList0 = [Ts| flowdata:fields(P, Fields, null)],
   DataList0;
build_value_stmt2(P = #data_point{}, Fields, _RemFieldsAs) ->
   DataList = build_value_stmt2(P, Fields, undefined),
   Rem = flowdata:to_map_except(P, [<<"ts">>|Fields]),
   DataList ++ [Rem].

build_batch([], _FieldList, _RemFieldsAs, _DedupQ, {DTag, PHashes, AccArgs}) ->
   {DTag, lists:reverse(PHashes), lists:reverse(AccArgs)};
build_batch([Point=#data_point{dtag = PointDTag}|Points], FieldList, RemFieldsAs, DedupQ, {_, PHashes, AccArgs}) ->
   PHash = erlang:phash2(Point#data_point{dtag = undefined}),
   NewAcc =
   case memory_queue:member(PHash, DedupQ) of
      true ->
         %% duplicate !!!
         lager:notice("duplicate item found, will drop it - ~p",[Point]),
         {PointDTag, PHashes, AccArgs};
      false ->
         {PointDTag, [PHash|PHashes], [build_value_stmt2(Point, FieldList, RemFieldsAs) | AccArgs]}
   end,
   build_batch(Points, FieldList, RemFieldsAs, DedupQ, NewAcc).



%% build base query
query_init(State = #state{table = DbTable}) when is_record(DbTable, faxe_lambda) ->
   State#state{query_from_lambda = true};
query_init(State = #state{table = Table, db_fields = DbFields, database = DB, remaining_fields_as = RemF}) ->
   FromLambda = lists:any(fun(F) -> faxe_lambda:is_lambda(F) end, DbFields),
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
   case faxe_lambda:is_lambda(Table0) of
      true ->
         TName = faxe_lambda:execute(P, Table0),
         <<DB/binary, ".", TName/binary>>;
      false -> Table0
   end,
   FieldsFun =
   fun(E) ->
      case faxe_lambda:is_lambda(E) of
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
      {ok, Message} = gun:await_body(Client, Ref),
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
%%handle_response(_, _) ->
%%%%   Err = <<"{\"error\":{\"message\":\"MaxBytesLengthExceededException[bytes can be at most 32766 in length; got 32910]\",\"code\":5000}}">>,
%%   Err = <<"{\"error\":{\"message\":\"wasistdas\",\"code\":5002}}">>,
%%   lager:error("Error ~p with body ~p",[500, Err]),
%%   case catch jiffy:decode(Err, [return_maps]) of
%%      #{<<"error">> := #{<<"code">> := Code, <<"message">> := ErrMsg}} ->
%%         crate_ignore_rules:check_ignore_error(Code, ErrMsg);
%%      _ -> {failed, server_error}
%%   end;
handle_response(<<"200">>, BodyJSON) ->
   handle_response_message(BodyJSON);
handle_response(<<"4", _/binary>> = S,_BodyJSON) ->
   lager:error("Error ~p: ~p",[S, _BodyJSON]),
   {error, invalid};
handle_response(<<"503">>, _BodyJSON) ->
   {failed, not_available};
handle_response(<<"5", _/binary>> = S, BodyJSON) ->
   lager:error("Error ~p with body ~p",[S, BodyJSON]),
   % <<"{\"error\":{\"message\":\"MaxBytesLengthExceededException[bytes can be at most 32766 in length; got 32910]\",\"code\":5000}}">>
   case catch jiffy:decode(BodyJSON, [return_maps]) of
      #{<<"error">> := #{<<"code">> := Code, <<"message">> := ErrMsg}} ->
         crate_ignore_rules:check_ignore_error(Code, ErrMsg);
      _ -> {failed, server_error}
   end;
handle_response({error, What}, {error, Reason}) ->
   lager:error("Other err: ~p",[{What, Reason}]),
   {failed, {What, Reason}}.

handle_response_message(RespMessage) ->
   Response = jiffy:decode(RespMessage, [return_maps]),
   case Response of
      #{<<"results">> := Results} ->
         %% count where rows := -2
         NotWritten = lists:foldl(
            fun
               (#{<<"rowcount">> := -2, <<"error_message">> := E}, {Idx, Count, Errors, _}) ->
                  {Idx + 1, Count + 1, Errors ++ [E], Idx};
               (#{<<"rowcount">> := -2}, {Idx, Count, Errors, _}) -> {Idx + 1, Count + 1, Errors, Idx};
               (_, {Idx, Count, Errors, CIdx}) -> {Idx + 1, Count, Errors, CIdx}
            end,
            {1, 0, [], 0}, Results),

         case NotWritten of
            {_Idx, 0, [], _} -> ok;
            {_Idx, C, Errs, ListIndex} ->
               lager:error("CrateDB: ~p of ~p rows not written, errors: ~p, will resend single (idx: ~p)",
                  [C, length(Results), Errs, ListIndex]),
               {error, retry_single, ListIndex}
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
   <<"\"", Identifier/binary, "\"">>;
quote_identifier(Other) ->
   Other.
