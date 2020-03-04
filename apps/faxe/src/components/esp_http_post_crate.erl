%% Date: 30.12.16 - 23:01
%% HTTP Post Request
%% Ⓒ 2019 heyoka
%%
-module(esp_http_post_crate).
-author("Alexander Minichmair").

-include("faxe.hrl").

-behavior(df_component).
%% API
-export([init/3, process/3, options/0, handle_info/2, do_send/4]).

-record(state, {
   host :: string(),
   port :: non_neg_integer(),
   path :: string(),
   key :: string(),
   client,
   database,
   table,
   query,
   db_fields,
   faxe_fields,
   remaining_fields_as,
   failed_retries,
   tries
}).

-define(KEY, <<"stmt">>).
-define(PATH, <<"/_sql">>).
-define(ARGS, <<"bulk_args">>).
-define(DEFAULT_SCHEMA_HDR, <<"Default-Schema">>).
-define(QUERY_TIMEOUT, 5000).
-define(FAILED_RETRIES, 3).

-define(HTTP_PROTOCOL, <<"http://">>).

options() ->
   [
      {host, string, {crate_http, host}},
      {port, integer, {crate_http, port}},
      {table, string},
      {database, string, <<"doc">>},
      {db_fields, string_list},
      {faxe_fields, string_list},
      {remaining_fields_as, string, undefined}].

init(_NodeId, _Inputs,
    #{host := Host0, port := Port, database := DB, table := Table,
       db_fields := DBFields, faxe_fields := FaxeFields, remaining_fields_as := RemFieldsAs}) ->

   Host1 = faxe_util:host_with_protocol(Host0, ?HTTP_PROTOCOL),
   Host = binary_to_list(Host1)++":"++integer_to_list(Port),

   {ok, C} = fusco:start(Host, []),
   erlang:monitor(process, C),
   Query = build_query(DBFields, Table, RemFieldsAs),
   {ok, all, #state{host = Host, port = Port, client = C, database = DB,
      failed_retries = ?FAILED_RETRIES, remaining_fields_as = RemFieldsAs,
      table = Table, query = Query, db_fields = DBFields, faxe_fields = FaxeFields}}.

%%% DATA IN
process(_In, DataItem, State = #state{}) ->
   NewState = send(DataItem, State),
   {ok, NewState}.

handle_info({'DOWN', _MonitorRef, _Type, Pid, _Info}, State = #state{client = Pid, host = Host}) ->
   lager:warning("fusco client is down, let's restart"),
   {ok, C} = fusco:start(Host, []),
   {ok, State#state{client = C}}.

%%% DATA OUT
send(Item, State = #state{query = Q, faxe_fields = Fields, remaining_fields_as = RemFieldsAs,
      database = Schema}) ->
   {Headers, Query} = build(Item, Q, Schema, Fields, RemFieldsAs),
   NewState = do_send(Query, Headers, 0, State),
   NewState.

build(Item, Query, Schema, Fields, RemFieldsAs) ->
   BulkArgs0 = build_value_stmt(Item, Fields, RemFieldsAs),
   BulkArgs =
      case Item of
         #data_point{} -> [BulkArgs0];
         _ -> BulkArgs0
      end,
   {
      [{?DEFAULT_SCHEMA_HDR, Schema}],
      jiffy:encode(#{?KEY => Query, ?ARGS => BulkArgs})
   }.

do_send(_Body, _Headers, MaxFailedRetries, S = #state{failed_retries = MaxFailedRetries}) ->
   lager:warning("could not send ~p with ~p retries", [_Body, MaxFailedRetries]),
   S;
do_send(Body, Headers, Retries, S = #state{client = Client}) ->
   Response = fusco:request(Client, ?PATH, "POST", Headers, Body, ?QUERY_TIMEOUT),
   case catch(handle_response(Response)) of
      ok ->
         S;

      {error, _} ->
         lager:warning("could not send ~p: invalid request", [Body]),
         S;

      _O ->
%%         lager:warning("sending problem :~p",[_O]),
         NewState =
         case is_process_alive(Client) of
            true ->
               S;
            false -> {ok, C} = fusco:start(S#state.host, []), S#state{client = C}
         end,
         do_send(Body, Headers, Retries+1, NewState)

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


-spec handle_response(tuple()) -> ok|{error, invalid}|{failed, term()}.
handle_response({ok,{{<<"200">>,<<"OK">>}, _Hdrs, _BodyJSON, _S, _T}}) ->
   ok;
handle_response({ok,{{<<"4", _/binary>>,_}, _Hdrs, _BodyJSON, _S, _T}}) ->
   lager:error("Error 400: ~p",[_BodyJSON]), {error, invalid};
handle_response({ok,{{<<"503">>,_}, _Hdrs, _BodyJSON, _S, _T}}) ->
   {failed, not_available};
handle_response({ok,{{<<"5", _/binary>>,_}, _Hdrs, _BodyJSON, _S, _T}}) ->
   lager:error("Error 400: ~p",[_BodyJSON]), {failed, server_error};
handle_response({error, What}) ->
   {failed, What}.