%%
%% HTTP GET Request
%% Ⓒ 2020 heyoka
%%
%%
-module(esp_http_get).
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
   uri_params :: map(),
   client,
   failed_retries,
   tries,
   tls,
   fn_id
}).

options() ->
   [
      {host, string},
      {port, integer},
      {path, string, <<"/">>},
      {param_keys, string_list, []},
      {param_values, string_list, []},
      {every, duration, undefined},
      {tls, is_set, false}
   ].

check_options() ->
   [
      {same_length, [param_keys, param_values]}
   ].

metrics() ->
   [
      {?METRIC_BYTES_SENT, meter, []}
   ].

init(NodeId, _Inputs, Opts =
    #{host := Host0, port := Port, path := Path, tls := Tls, param_keys := _Keys, param_values := _Vals}) ->
   lager:notice("OPTS: ~p",[Opts]),
   Host = binary_to_list(Host0),
   connection_registry:reg(NodeId, Host, Port, <<"http">>),
   erlang:send_after(0, self(), start_client),
   {ok, all,
      #state{
         host = Host, port = Port,
         path = Path, tls = Tls,
         failed_retries = ?FAILED_RETRIES,
         fn_id = NodeId}}.

process(_In, #data_point{}, State = #state{}) ->
   _Res = request(State),
   {ok, State};
process(_In, #data_batch{}, State = #state{}) ->
   _Res = request(State),
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

request(_State = #state{client = Client, path = Path}) ->
%%   Headers = [{<<"Accept">>, <<"application/json">>}, {<<"Cache-Control">>, {<<"max-age=0">>}}],
   %Headers = [{<<"Authorization">>, <<"Basic ">>}],
   StreamRef = gun:get(Client, Path, []),
   case gun:await(Client, StreamRef) of
      {response, fin, Status, _Headers} ->
         lager:notice("Response Status: ~p",[Status]),
         no_data;
      {response, nofin, Status, _Headers} ->
         {ok, Body} = gun:await_body(Client, StreamRef),
         lager:notice("~s~n", [Body]),
         handle_response(integer_to_binary(Status), Body)
   end.



start_client(State = #state{host = Host, port = Port, tls = Tls}) ->
   lager:info("start client: ~n~p",[State]),
   connection_registry:connecting(),
   Opts = #{connect_timeout => 3000},
   Options =
   case Tls of
      true -> Opts#{transport => tls};
      false -> Opts
   end,
   {ok, C} = gun:open(Host, Port, Options),
   MonitorRef = erlang:monitor(process, C),
   case gun:await_up(C) of
      {ok, _} ->
         connection_registry:connected(),
         State#state{client = C};
      E -> lager:warning("~p connecting to ~p:~p", [E, Host, Port]),
         erlang:demonitor(MonitorRef),
         erlang:send_after(1000, self(), start_client), State#state{client = undefined}
   end.


-spec handle_response(integer(), binary()) -> ok|{error, invalid}|{failed, term()}.
handle_response(<<"2", _/binary>>, BodyJSON) ->
   {ok, BodyJSON};
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