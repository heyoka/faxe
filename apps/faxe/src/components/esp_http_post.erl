%% Date: 30.12.16 - 23:01
%% HTTP Post Request
%% Ⓒ 2019 heyoka
%%
%%
-module(esp_http_post).
-author("Alexander Minichmair").

-include("faxe.hrl").

-behavior(df_component).
%% API
-export([init/3, process/3, options/0, handle_info/2, shutdown/1, metrics/0, check_options/0]).

-define(FAILED_RETRIES, 3).

-record(state, {
   host :: string(),
   port :: non_neg_integer(),
   path :: string(),
   auth :: undefined|binary(),
   headers :: list(),
   client,
   failed_retries,
   without :: list(),
   field :: binary(),
   tries,
   tls,
   fn_id
}).

options() ->
   [
      {host, string},
      {port, integer},
      {path, string, <<"/">>},
      {tls, is_set, false},
      {user, string, undefined},
      {pass, string, undefined},
      {header_names, string_list, []},
      {header_values, string_list, []},
      {field, string, undefined},
      {without, string_list, []}
   ].

check_options() ->
   [{same_length, [header_names, header_values]}].

metrics() ->
   [
      {?METRIC_BYTES_SENT, meter, []}
   ].

init(NodeId, _Inputs, #{host := Host0, port := Port, path := Path, tls := Tls, without := Without, field := Field,
      user := User, pass := Pass, header_names := CustomHeaderNames, header_values := CustomHeaderValues}) ->
   Host = binary_to_list(Host0),
   connection_registry:reg(NodeId, Host, Port, <<"http">>),
   erlang:send_after(0, self(), start_client),
   Headers0 =
      [{<<"content-type">>, <<"application/json">>}, {<<"accept">>, <<"application/json">>}] ++
      http_lib:basic_auth_header(User, Pass) ++ http_lib:user_agent_header(),
   Headers = Headers0 ++ lists:zip(CustomHeaderNames, CustomHeaderValues),
%%   lager:notice("all headers are ~p",[Headers]),
   {ok, all,
      #state{host = Host, port = Port, path = Path, tls = Tls, without = Without, field = Field,
         failed_retries = ?FAILED_RETRIES, fn_id = NodeId, headers = Headers}}.

process(_In, P = #data_point{}, State = #state{}) ->
   send(P, State),
   {ok, State};
process(_In, B = #data_batch{}, State = #state{}) ->
   send(B, State),
   {ok, State}.

handle_info({'DOWN', _MonitorRef, _Type, Pid, _Info}, State = #state{client = Pid}) ->
   connection_registry:disconnected(),
   lager:warning("gun is down"),
   handle_info(start_client, State);
handle_info(start_client, State) ->
   NewState = start_client(State),
   {ok, NewState};
handle_info(_R, State) ->
   {ok, State}.

shutdown(#state{client = C}) ->
   gun:close(C).

send(Item, State = #state{headers = Headers, without = Without, field = Field}) ->
   M0 =
   case Field of
      undefined -> flowdata:to_mapstruct(Item);
      _ -> flowdata:field(Item, Field)
   end,
   M = maybe_without(Without, M0),
   Body = jiffy:encode(M),
%%   lager:notice("Body to send ~p",[Body]),
   do_send(Body, Headers, 0, State).

do_send(_Body, _Headers, MaxFailedRetries, S = #state{failed_retries = MaxFailedRetries}) ->
   lager:warning("could not send ~p with ~p retries", [_Body, MaxFailedRetries]),
   S;
do_send(Body, Headers, Retries, S = #state{client = Client, path = Path, fn_id = FNId}) ->
   Ref = gun:post(Client, Path, Headers, Body),
   case catch(get_response(Client, Ref)) of
      ok ->
         node_metrics:metric(?METRIC_ITEMS_OUT, 1, FNId),
         node_metrics:metric(?METRIC_BYTES_SENT, byte_size(Body), FNId),
         S;
      {error, What} ->
         lager:warning("could not send ~p: invalid request: ~p", [Body, What]),
         S;
      O ->
         lager:notice("Problem sending data: ~p",[O]),
         do_send(Body, Headers, Retries+1, S)

   end.

start_client(State = #state{host = Host, port = Port, tls = Tls}) ->
   connection_registry:connecting(),
   Opts = #{connect_timeout => 5000},
   Options =
   case Tls of
      true -> Opts#{transport => tls};
      false -> Opts
   end,
   {ok, C} = gun:open(Host, Port, Options),
   erlang:monitor(process, C),
   case gun:await_up(C) of
      {ok, _} ->
         connection_registry:connected(),
         State#state{client = C};
      E -> lager:warning("~p connecting to ~p:~p", [E, Host, Port]),
         erlang:send_after(1000, self(), start_client), State#state{client = undefined}
   end.

get_response(Client, Ref) ->
   case gun:await(Client, Ref) of
      {response, fin, Status, _Headers} ->
         {error, {no_data, Status}};
      {response, nofin, Status, _Headers} ->
         {ok, Body} = gun:await_body(Client, Ref),
         handle_response(integer_to_binary(Status), Body);
      Other -> Other

   end.

-spec handle_response(integer(), binary()) -> ok|{error, invalid}|{failed, term()}.
handle_response(<<"2", _/binary>>, _BodyJSON) ->
   ok;
handle_response(<<"4", _/binary>>,_BodyJSON) ->
   lager:error("Error 400: ~p",[_BodyJSON]),
   {error, invalid};
handle_response(<<"503">>, _BodyJSON) ->
   {failed, not_available};
handle_response(<<"5", _/binary>>,_BodyJSON) ->
   lager:error("Error 5xx: ~p",[_BodyJSON]),
   {failed, server_error};
handle_response({error, What}, {error, Reason}) ->
   {failed, {What, Reason}}.

maybe_without([], Data) ->
   Data;
maybe_without(Without, Data) when is_map(Data) ->
   maps:without(Without, Data);
maybe_without(Without, Data) when is_list(Data) ->
   maps:without(Without, Data).
