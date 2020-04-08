%% Date: 30.12.16 - 23:01
%% HTTP Post Request
%% Ⓒ 2019 heyoka
%%
%% @todo retry on error
%%
-module(esp_http_post).
-author("Alexander Minichmair").

-include("faxe.hrl").

-behavior(df_component).
%% API
-export([init/3, process/3, options/0, handle_info/2, shutdown/1]).

-define(FAILED_RETRIES, 3).

-record(state, {
   host :: string(),
   port :: non_neg_integer(),
   path :: string(),
   client,
   failed_retries,
   tries,
   tls
}).

options() ->
   [
      {host, string},
      {port, integer},
      {path, string, <<"/">>},
      {tls, is_set, false}
   ].

init(_NodeId, _Inputs, #{host := Host0, port := Port, path := Path, tls := Tls}) ->
   Host = binary_to_list(Host0),
   erlang:send_after(0, self(), start_client),
   {ok, all,
      #state{host = Host, port = Port, path = Path, tls = Tls, failed_retries = ?FAILED_RETRIES}}.

process(_In, P = #data_point{}, State = #state{}) ->
   send(P, State),
   {ok, State};
process(_In, B = #data_batch{}, State = #state{}) ->
   send(B, State),
   {ok, State}.

handle_info({'DOWN', _MonitorRef, _Type, Pid, _Info}, State = #state{client = Pid}) ->
   lager:warning("gun is down"),
   handle_info(start_client, State);
handle_info(start_client, State) ->
   NewState = start_client(State),
   {ok, NewState}.

shutdown(#state{client = C}) ->
   gun:close(C).

send(Item, State = #state{}) ->
   M = flowdata:to_mapstruct(Item),
   Body = jiffy:encode(M),
   Headers = [{<<"content-type">>, <<"application/json">>}],
   do_send(Body, Headers, 0, State).

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
%%         lager:warning("sending problem :~p",[_O]),
%%         NewState =
%%         case is_process_alive(Client) of
%%            true ->
%%               S;
%%            false -> {ok, C} = fusco:start(S#state.host, []), S#state{client = C}
%%         end,
         do_send(Body, Headers, Retries+1, S)

   end.

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

get_response(Client, Ref) ->
   {response, _IsFin, Status, _Headers} = _Res = gun:await(Client, Ref),
%%   lager:notice("resp-res: ~p",[Res]),
   {ok, Message} = gun:await_body(Client, Ref),
%%   lager:notice("resp-body: ~p",[Message]),
   handle_response(integer_to_binary(Status), Message).

-spec handle_response(integer(), binary()) -> ok|{error, invalid}|{failed, term()}.
handle_response(<<"2", _/binary>>, _BodyJSON) ->
   ok;
handle_response(<<"4", _/binary>>,_BodyJSON) ->
   lager:error("Error 400: ~p",[_BodyJSON]), {error, invalid};
handle_response(<<"503">>, _BodyJSON) ->
   {failed, not_available};
handle_response(<<"5", _/binary>>,_BodyJSON) ->
   lager:error("Error 400: ~p",[_BodyJSON]), {failed, server_error};
handle_response({error, What}, {error, Reason}) ->
   {failed, {What, Reason}}.