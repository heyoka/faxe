%% Date: 30.12.16 - 23:01
%% CrateDB Writer that uses crate's http endpoint
%% Ⓒ 2019 heyoka
%%
-module(esp_crate_out).
-author("Alexander Minichmair").

-include("faxe.hrl").

-behavior(df_component).
%% API
-export([init/3, process/3, options/0, handle_info/2, do_send/5, shutdown/1, metrics/0, check_options/0]).

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
   db_fields,
   faxe_fields,
   remaining_fields_as,
   failed_retries,
   tries,
   tls,
   fn_id,
   last_error,
   debug_mode = false,
   flow_inputs
}).

-define(KEY, <<"stmt">>).
-define(PATH, <<"/_sql?error_trace=true">>).
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
      {db_fields, list},
      {faxe_fields, string_list},
      {remaining_fields_as, string, undefined},
      {max_retries, integer, ?FAILED_RETRIES}
   ].

check_options() ->
   [
      {func, table, fun(E) -> is_function(E) orelse is_binary(E) end, <<" must be either a string or a lambda function">>},
      {func, db_fields,
         fun(Fields) ->
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
       tls := Tls, db_fields := DBFields, faxe_fields := FaxeFields,
       remaining_fields_as := RemFieldsAs, max_retries := MaxRetries}) ->

   Host = binary_to_list(Host0),
   erlang:send_after(0, self(), start_client),
%%   Query = maybe_build_query(DBFields, Table, RemFieldsAs),
   connection_registry:reg(NodeId, Host, Port, <<"http">>),
   State = #state{host = Host, port = Port, database = DB, user = User, pass = Pass,
      failed_retries = MaxRetries, remaining_fields_as = RemFieldsAs, tls = Tls,
      table = Table, db_fields = DBFields, faxe_fields = FaxeFields,
      fn_id = NodeId, flow_inputs = Inputs},
   NewState = query_init(State),
%   lager:warning("QUERY INIT: ~p",[{NewState#state.query, NewState#state.query_from_lambda}]),
   {ok, all, NewState}.

%%% DATA IN
%% not connected -> drop message
%% @todo buffer these messages when not connected
process(_In, _DataItem, State = #state{client = undefined}) ->
   {ok, State};
%% we do not have a prepare query yet
process(_In, DataItem=#data_batch{points = _Points},
    State = #state{query_from_lambda = true, table = Table, db_fields = DbFields, remaining_fields_as = RemFields}) ->
%%   lager:warning("last point: ~p",[lager:pr(lists:last(Points), ?MODULE)]),
   Point = get_query_point(DataItem),
   Query = build_query(DbFields, Table, RemFields, Point),
   do_process(DataItem, State#state{query = Query});
process(_In, DataItem, State) ->
   do_process(DataItem, State).

do_process(DataItem, State = #state{fn_id = FNId}) ->
   _NewState = send(DataItem, State),
%%   dataflow:maybe_debug(item_in, 1, DataItem, FNId, State#state.debug_mode),
   {ok, State}.

handle_info({'DOWN', _MonitorRef, _Type, Pid, _Info}, State = #state{client = Pid}) ->
   connection_registry:disconnected(),
   lager:warning("gun is down"),
   handle_info(start_client, State#state{client = undefined});
handle_info(start_client, State) ->
   NewState = start_client(State),
   {ok, NewState};
handle_info(start_debug, State) -> {ok, State#state{debug_mode = true}};
handle_info(stop_debug, State) -> {ok, State#state{debug_mode = false}};
handle_info(_Req, State) ->
   {ok, State}.

shutdown(#state{client = C}) ->
   gun:close(C).

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

get_query_point(#data_batch{points = [P|_]}) ->
   P;
get_query_point(#data_point{} = P) ->
   P.

%%% DATA OUT
send(Item, State = #state{query = Q, faxe_fields = Fields, remaining_fields_as = RemFieldsAs,
      database = Schema, user = User, pass = Pass}) ->
   Query = build(Item, Q, Fields, RemFieldsAs),
   Headers0 = [{?DEFAULT_SCHEMA_HDR, Schema}, {<<"content-type">>, <<"application/json">>}],
   Headers =
   case Pass of
      undefined -> Headers0;
      _ when is_binary(Pass) andalso is_binary(User) -> UP = <<User/binary, ":", Pass/binary>>,
         Auth = base64:encode(UP),
         Headers0 ++ [{?AUTH_HEADER_KEY, <<"Basic ", Auth/binary>>}];
      _ -> Headers0
   end,
   NewState = do_send(Item, Query, Headers, 0, State#state{last_error = undefined}),
   MBytes = faxe_util:bytes(Query),
   node_metrics:metric(?METRIC_BYTES_SENT, MBytes, State#state.fn_id),
   node_metrics:metric(?METRIC_ITEMS_OUT, 1, State#state.fn_id),
   NewState.

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
do_send(Item, Body, Headers, Retries, State = #state{client = Client, fn_id = FNId}) ->
   Ref = gun:post(Client, ?PATH, Headers, Body),
   case catch(get_response(Client, Ref)) of
      ok ->
         dataflow:ack(Item, State#state.flow_inputs),
         dataflow:maybe_debug(item_out, 1, Item, FNId, State#state.debug_mode),
         State;
      {error, _} ->
         lager:warning("could not send ~p: invalid request", [Body]),
         State;

      O ->
         lager:warning("sending gun post: ~p",[O]),
         do_send(Item, Body, Headers, Retries+1, State#state{last_error = O})
   end.


build_value_stmt(_B = #data_batch{points = Points}, Fields, RemFieldsAs) ->
   build_batch(Points, Fields, RemFieldsAs, []);
build_value_stmt(P = #data_point{ts = Ts}, Fields, undefined) ->
   DataList0 = [Ts| flowdata:fields(P, Fields)],
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
query_init(State = #state{table = Table, db_fields = DbFields, remaining_fields_as = RemF}) ->
   FromLambda = lists:any(fun(F) -> is_function(F) end, DbFields),
   case FromLambda of
      false ->
         Query = build_query(DbFields, Table, RemF),
         State#state{query = Query, query_from_lambda = false};
      true ->
         State#state{query_from_lambda = FromLambda}
   end.


%% build the query with lambda funs
build_query(DbFields0, Table0, RemFieldsAs, P=#data_point{}) when is_list(DbFields0) ->
%%   lager:warning("building query with lambdas maybe: (~p, ~p, ~p)",[DbFields0, Table0, flowdata:field(P, <<"meta">>)]),
   Table =
   case is_function(Table0) of
      true -> faxe_lambda:execute(P, Table0);
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

get_response(Client, Ref) ->
   {response, _IsFin, Status, _Headers} = gun:await(Client, Ref, ?QUERY_TIMEOUT),
%%   lager:info("response Status: ~p, Headers: ~p" ,[Status, _Headers]),
   {ok, Message} = gun:await_body(Client, Ref),
%%   handle_response_message(Message),
   handle_response(integer_to_binary(Status), Message).

-spec handle_response(integer(), binary()) -> ok|{error, invalid}|{failed, term()}.
handle_response(<<"200">>, BodyJSON) ->
   handle_response_message(BodyJSON),
   ok;
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
   case Response of
      #{<<"results">> := Results} ->
         %% count where rows := -2
         NotWritten = lists:foldl(
            fun
               (#{<<"rowcount">> := -2}, Acc) -> Acc+1;
               (_, Acc) -> Acc
            end, 0, Results),
         case NotWritten of
            0 -> ok;
            _ ->
%%               lager:info("results ~p",[Results]),
               lager:notice("CrateDB: ~p of ~p rows not written",[length(Results), NotWritten])
         end
      end.
