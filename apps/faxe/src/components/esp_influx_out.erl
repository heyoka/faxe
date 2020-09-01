%% Date: 30.12.16 - 23:01
%% write data to influxdb via it's http API
%% Ⓒ 2020 heyoka
%%
-module(esp_influx_out).
-author("Alexander Minichmair").

-include("faxe.hrl").

-behavior(df_component).
%% API
-export([init/3, process/3, options/0, handle_info/2, do_send/4, shutdown/1, metrics/0]).

-record(state, {
   host :: string(),
   port :: non_neg_integer(),
   path :: string(),
   user :: binary(),
   pass :: binary(),
   key :: string(),
   client,
   database,
   measurement,
   retention_policy,
   failed_retries,
   tries,
   tls,
   fn_id
}).

-define(PATH, <<"/write?db=">>).

-define(AUTH_HEADER_KEY, <<"Authorization">>).
-define(QUERY_TIMEOUT, 5000).
-define(FAILED_RETRIES, 3).

-define(CONNECT_OPTS, #{transport => tls, connect_timeout => 3000}).


options() ->
   [
      {host, string, {influx_http, host}},
      {port, integer, {influx_http, port}},
      {tls, is_set, false},
      {user, string, {influx_http, user}},
      {pass, string, {influx_http, pass}},
      {database, string},
      {measurement, string},
      {retpol, string, undefined}
   ].

metrics() ->
   [
      {?METRIC_BYTES_SENT, meter, []}
   ].

init(NodeId, _Inputs,
    #{host := Host0, port := Port, database := DB, user := User, pass := Pass,
       tls := Tls, measurement := Measure, retpol := RetPol
    }) ->

   Host = binary_to_list(Host0),
   erlang:send_after(0, self(), start_client),

   connection_registry:reg(NodeId, Host, Port, <<"http">>),
   S = #state{host = Host, port = Port, database = DB, user = User, pass = Pass,
      failed_retries = ?FAILED_RETRIES, measurement = Measure, retention_policy = RetPol,
      tls = Tls, fn_id = NodeId},

   Path = build_path(S),
   {ok, all, S#state{path = Path}}.

%%% DATA IN
%% not connected -> drop message
%% @todo buffer these messages when not connected -> esq
process(_In, _DataItem, State = #state{client = undefined}) ->
   {ok, State};
process(_In, DataItem, State = #state{}) ->
   _NewState = send(DataItem, State),
   {ok, State}.

handle_info({'DOWN', _MonitorRef, _Type, Pid, _Info}, State = #state{client = Pid}) ->
   connection_registry:disconnected(),
   lager:warning("gun is down"),
   handle_info(start_client, State#state{client = undefined});
handle_info(start_client, State) ->
   NewState = start_client(State),
   {ok, NewState};
handle_info(_Req, State) ->
   {ok, State}.

shutdown(#state{client = C}) ->
   gun:close(C).

start_client(State = #state{host = Host, port = Port, tls = Tls}) ->
   connection_registry:connecting(),
   Opts = #{connect_timeout => 3000},
   Options =
      case Tls of
         true -> Opts#{transport => tls};
         false -> Opts
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

%%% DATA OUT
send(Item, State = #state{
%%   faxe_fields = Fields, remaining_fields_as = RemFieldsAs, user = User, pass = Pass,
   measurement = Measurement}) ->
   Body = build(Item, Measurement), % Fields, RemFieldsAs),
   Headers0 = [{<<"content-type">>, <<"application/json">>}],
   Headers = Headers0,
%%   case Pass of
%%      undefined -> Headers0;
%%      _ -> UP = <<User/binary, ":", Pass/binary>>,
%%         Auth = base64:encode(UP),
%%         Headers0 ++ [{?AUTH_HEADER_KEY, <<"Basic ", Auth/binary>>}]
%%   end,
   NewState = do_send(Body, Headers, 0, State),
   node_metrics:metric(?METRIC_BYTES_SENT, iolist_size(Body), State#state.fn_id),
   node_metrics:metric(?METRIC_ITEMS_OUT, 1, State#state.fn_id),
   NewState.

build(Item, Measurement) -> %, Fields, RemFieldsAs) ->
   Lines = encode(Item, Measurement),
   Lines.

do_send(_Body, _Headers, MaxFailedRetries, S = #state{failed_retries = MaxFailedRetries}) ->
   lager:warning("could not send ~p with ~p retries", [_Body, MaxFailedRetries]),
   S;
do_send(Body, Headers, Retries, S = #state{client = Client, path = Path}) ->
   Ref = gun:post(Client, Path, Headers, Body),
   case catch(get_response(Client, Ref)) of
      ok ->
         S;
      {error, _} ->
         lager:warning("could not send ~p: invalid request", [Body]),
         S;

      _O ->
         lager:warning("sending problem :~p",[_O]),
         do_send(Body, Headers, Retries+1, S)
   end.


build_path(S = #state{database = DB}) ->
   Path0 = [?PATH, DB, "&precision=ms"],
   Path1 = maybe_add_qs_auth(Path0, S),
   maybe_add_rp(Path1, S).

maybe_add_qs_auth(Path, #state{user = undefined}) ->
   Path;
maybe_add_qs_auth(Path, #state{user = U, pass = P}) ->
   [Path, "&u=", U, "&p=", P].
maybe_add_rp(Path, #state{retention_policy = undefined}) ->
   Path;
maybe_add_rp(Path, #state{retention_policy = RP}) ->
   [Path, "&rp=", RP].

get_response(Client, Ref) ->
   {response, _IsFin, Status, _Headers} = gun:await(Client, Ref),
   case Status of
      204 -> handle_response(Status);
      _ ->
         {ok, Message} = gun:await_body(Client, Ref),
         handle_response(integer_to_binary(Status), Message)
   end.


handle_response(204) ->
   ok.
-spec handle_response(integer(), binary()) -> ok|{error, invalid}|{failed, term()}.
handle_response(<<"20", _/binary>>, _BodyJson) ->
   ok;
handle_response(<<"4", _/binary>>,_BodyJSON) ->
   lager:error("Error 400: ~p",[_BodyJSON]), {error, invalid};
handle_response(<<"503">>, _BodyJSON) ->
   {failed, not_available};
handle_response(<<"5", _/binary>>,_BodyJSON) ->
   lager:error("Error 400: ~p",[_BodyJSON]), {failed, server_error};
handle_response({error, What}, {error, Reason}) ->
   {failed, {What, Reason}}.


encode(#data_point{ts = Ts, fields = Fields, tags = Tags}, Measurement) ->
   influx_line:encode({Measurement, filter_fields(Fields), filter_fields(Tags), Ts});
encode(#data_batch{points = Ps}, Measurement) ->
   Data = [
      {Measurement,
      filter_fields(P#data_point.fields),
      filter_fields(P#data_point.tags), P#data_point.ts} || P <- Ps],
   influx_line:encode(Data).

%% filter fields or tags to only have valid values
filter_fields(Fields) ->
   maps:filter(fun(_K, V) -> not is_map(V) andalso not is_list(V) end, Fields).