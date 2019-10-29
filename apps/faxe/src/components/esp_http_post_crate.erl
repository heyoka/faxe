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
   failed_retries,
   tries
}).

-define(KEY, <<"stmt">>).
-define(PATH, <<"/_sql">>).
-define(ARGS, <<"bulk_args">>).
-define(DEFAULT_SCHEMA_HDR, <<"Default-Schema">>).
-define(QUERY_TIMEOUT, 5000).
-define(FAILED_RETRIES, 3).

options() ->
   [
      {host, string},
      {port, integer},
      {table, string},
      {database, string, <<"doc">>},
      {db_fields, string_list},
      {faxe_fields, string_list}].

init(_NodeId, _Inputs,
    #{host := Host0, port := Port, database := DB, table := Table,
       db_fields := DBFields, faxe_fields := FaxeFields}) ->

   Host = binary_to_list(Host0)++":"++integer_to_list(Port),
   {ok, C} = fusco:start(Host, []),
   erlang:monitor(process, C),
   Query = build_query(DBFields, Table),
%%   lager:notice("Query: ~p", [Query]),
   {ok, all, #state{host = Host, port = Port, client = C, database = DB,
      failed_retries = ?FAILED_RETRIES,
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
send(Item, State = #state{query = Q, faxe_fields = Fields, database = Schema}) ->
   {Headers, Query} = build(Item, Q, Schema, Fields),
   NewState = do_send(Query, Headers, 0, State),
   NewState.

build(Item, Query, Schema, Fields) ->
   BulkArgs0 = build_value_stmt(Item, Fields),
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

      _ ->
         NewState =
         case is_process_alive(Client) of
            true ->
               S;
            false -> {ok, C} = fusco:start(S#state.host, []), S#state{client = C}
         end,
         do_send(Body, Headers, Retries+1, NewState)

   end.


build_value_stmt(_B = #data_batch{points = Points}, Fields) ->
   build_batch(Points, Fields, []);
build_value_stmt(P = #data_point{ts = Ts}, Fields) ->
   DataList0 = [Ts| flowdata:fields(P, Fields)],
   DataList0.

build_batch([], _FieldList, Acc) ->
   Acc;
build_batch([Point|Points], FieldList, Acc) ->
   NewAcc = [build_value_stmt(Point, FieldList) | Acc],
   build_batch(Points, FieldList, NewAcc).

build_query(ValueList0, Table) when is_list(ValueList0) ->
   Q0 = <<"INSERT INTO ", Table/binary>>,
   ValueList = [<<"ts">>|ValueList0],
   Fields = iolist_to_binary(lists:join(<<", ">>, ValueList)),
   Q1 = <<Q0/binary, " (", Fields/binary, ") VALUES ">>,
   QMarks = iolist_to_binary(lists:join(<<", ">>, lists:duplicate(length(ValueList), "?"))),
   Q = <<Q1/binary, "(", QMarks/binary, ")">>,
   Q.


-spec handle_response(tuple()) -> ok|{error, invalid}|{failed, term()}.
handle_response({ok,{{<<"200">>,<<"OK">>}, _Hdrs, _BodyJSON, _S, _T}}) -> ok;
handle_response({ok,{{<<"4", _/binary>>,_}, _Hdrs, _BodyJSON, _S, _T}}) ->
   lager:error("Error 400: ~p",[_BodyJSON]), {error, invalid};
handle_response({ok,{{<<"503">>,_}, _Hdrs, _BodyJSON, _S, _T}}) -> {failed, not_available};
handle_response({ok,{{<<"5", _/binary>>,_}, _Hdrs, _BodyJSON, _S, _T}}) -> {failed, server_error};
handle_response({error, What}) -> {failed, What}.