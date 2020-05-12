%% Date: 30.12.16 - 23:01
%% HTTP Post Request
%% Ⓒ 2019 heyoka
%%
-module(esp_crate_out).
-author("Alexander Minichmair").

-include("faxe.hrl").

-behavior(df_component).
%% API
-export([init/3, process/3, options/0, handle_info/2, do_send/4, shutdown/1]).

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
   query,
   db_fields,
   faxe_fields,
   remaining_fields_as,
   failed_retries,
   tries,
   tls
}).

-define(KEY, <<"stmt">>).
-define(PATH, <<"/_sql">>).
-define(ARGS, <<"bulk_args">>).
-define(DEFAULT_SCHEMA_HDR, <<"Default-Schema">>).
-define(AUTH_HEADER_KEY, <<"Authorization">>).
-define(QUERY_TIMEOUT, 5000).
-define(FAILED_RETRIES, 3).

-define(CONNECT_OPTS, #{transport => tls, connect_timeout => 3000}).


options() ->
   [
      {host, string, {crate_http, host}},
      {port, integer, {crate_http, port}},
      {tls, is_set, false},
      {table, string},
      {user, string, {crate_http, user}},
      {pass, string, {crate_http, pass}},
      {database, string, <<"doc">>},
      {db_fields, string_list},
      {faxe_fields, string_list},
      {remaining_fields_as, string, undefined}].

init(_NodeId, _Inputs,
    #{host := Host0, port := Port, database := DB, table := Table, user := User, pass := Pass,
       tls := Tls,
       db_fields := DBFields, faxe_fields := FaxeFields, remaining_fields_as := RemFieldsAs}) ->

   Host = binary_to_list(Host0),
   erlang:send_after(0, self(), start_client),
   Query = build_query(DBFields, Table, RemFieldsAs),
   {ok, all, #state{host = Host, port = Port, database = DB, user = User, pass = Pass,
      failed_retries = ?FAILED_RETRIES, remaining_fields_as = RemFieldsAs, tls = Tls,
      table = Table, query = Query, db_fields = DBFields, faxe_fields = FaxeFields}}.

%%% DATA IN
%% not connected -> drop message
process(_In, _DataItem, State = #state{client = undefined}) ->
   {ok, State};
process(_In, DataItem, State = #state{}) ->
   _NewState = send(DataItem, State),
   {ok, State}.

handle_info({'DOWN', _MonitorRef, _Type, Pid, _Info}, State = #state{client = Pid}) ->
   lager:warning("gun is down"),
   handle_info(start_client, State);
handle_info(start_client, State) ->
   NewState = start_client(State),
   {ok, NewState};
handle_info(_Req, State) ->
   {ok, State}.

shutdown(#state{client = C}) ->
   gun:close(C).

start_client(State = #state{host = Host, port = Port, tls = Tls}) ->
   Opts = #{connect_timeout => 3000},
   Options =
      case Tls of
         true -> Opts#{transport => tls};
         false -> Opts
      end,
   {ok, C} = gun:open(Host, Port, Options),
   erlang:monitor(process, C),
   case gun:await_up(C) of
      {ok, _} -> State#state{client = C};
      _ -> lager:warning("timeout connecting to ~p:~p", [Host, Port]),
         erlang:send_after(1000, self(), start_client), State#state{client = undefined}
   end.


%%% DATA OUT
send(Item, State = #state{query = Q, faxe_fields = Fields, remaining_fields_as = RemFieldsAs,
      database = Schema, user = User, pass = Pass}) ->
   Query = build(Item, Q, Fields, RemFieldsAs),
   Headers0 = [{?DEFAULT_SCHEMA_HDR, Schema}, {<<"content-type">>, <<"application/json">>}],
   Headers =
   case Pass of
      undefined -> Headers0;
      _ -> UP = <<User/binary, ":", Pass/binary>>,
         Auth = base64:encode(UP),
         Headers0 ++ [{?AUTH_HEADER_KEY, <<"Basic ", Auth/binary>>}]
   end,
   NewState = do_send(Query, Headers, 0, State),
   NewState.

build(Item, Query, Fields, RemFieldsAs) ->
   BulkArgs0 = build_value_stmt(Item, Fields, RemFieldsAs),
   BulkArgs =
      case Item of
         #data_point{} -> [BulkArgs0];
         _ -> BulkArgs0
      end,
   jiffy:encode(#{?KEY => Query, ?ARGS => BulkArgs}).

do_send(_Body, _Headers, MaxFailedRetries, S = #state{failed_retries = MaxFailedRetries}) ->
   lager:warning("could not send ~p with ~p retries", [_Body, MaxFailedRetries]),
   S;
do_send(Body, Headers, Retries, S = #state{client = Client}) ->
   Ref = gun:post(Client, ?PATH, Headers, Body),
   case catch(get_response(Client, Ref)) of
      ok ->
         S;

      {error, _} ->
         lager:warning("could not send ~p: invalid request", [Body]),
         S;

      _O ->
%%         lager:warning("sending problem :~p",[_O]),
%%         NewState =
%%         case is_process_alive(Client) of
%%            true ->
%%               S;
%%            false -> {ok, C} = fusco:start(S#state.host, []), S#state{client = C}
%%         end,
         do_send(Body, Headers, Retries+1, S)

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


get_response(Client, Ref) ->
   {response, _IsFin, Status, _Headers} = gun:await(Client, Ref),
   {ok, Message} = gun:await_body(Client, Ref),
   handle_response(integer_to_binary(Status), Message).

-spec handle_response(integer(), binary()) -> ok|{error, invalid}|{failed, term()}.
handle_response(<<"200">>, _BodyJSON) ->
   ok;
handle_response(<<"4", _/binary>>,_BodyJSON) ->
   lager:error("Error 400: ~p",[_BodyJSON]), {error, invalid};
handle_response(<<"503">>, _BodyJSON) ->
   {failed, not_available};
handle_response(<<"5", _/binary>>,_BodyJSON) ->
   lager:error("Error 400: ~p",[_BodyJSON]), {failed, server_error};
handle_response({error, What}, {error, Reason}) ->
   {failed, {What, Reason}}.