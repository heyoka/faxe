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
-define(P_TYPE_PLAIN, <<"plain">>).
-define(P_TYPE_JSON, <<"json">>).


-record(state, {
   host :: string(),
   port :: non_neg_integer(),
   path :: string(),
   every,
   align :: true|false,
   as :: binary(),
   payload_type = <<"plain">> :: binary(),
   timer :: undefined|#faxe_timer{},
   uri_params :: map(),
   client,
   failed_retries,
   retries,
   tls,
   fn_id
}).

options() ->
   [
      {host, string},
      {port, integer},
      {path, string, <<"/">>},
      {payload_type, string, ?P_TYPE_JSON},
      {param_keys, string_list, []},
      {param_values, string_list, []},
      {every, duration, undefined},
      {align, is_set, false},
      {tls, is_set, false},
      {retries, pos_integer, 2},
      {as, string, undefined}
   ].

check_options() ->
   [
      {same_length, [param_keys, param_values]}
   ].

metrics() ->
   [
      {?METRIC_BYTES_SENT, meter, []}
   ].

init(NodeId, _Inputs,
    #{host := Host0, port := Port, path := Path, tls := Tls, param_keys := _Keys, param_values := _Vals,
       every := Every, align := Align, retries := Retries, as := As, payload_type := PType}) ->

   Host = binary_to_list(Host0),
   connection_registry:reg(NodeId, Host, Port, <<"http">>),
   Alias =
      case {As, PType} of
         {undefined, ?P_TYPE_PLAIN} -> <<"data">>;
         _ -> As
      end,
   erlang:send_after(0, self(), start_client),
   {ok, all,
      #state{
         host = Host, port = Port,
         path = Path, tls = Tls,
         every = Every, align = Align,
         retries = Retries, as = Alias,
         payload_type = PType,
         failed_retries = ?FAILED_RETRIES,
         fn_id = NodeId}}.

process(_In, _Item, State = #state{}) ->
   try_request(faxe_time:now(), State).

handle_info(request, State = #state{timer = Timer}) ->
   NewTimer = faxe_time:timer_next(Timer),
   try_request(Timer#faxe_timer.last_time, State#state{timer = NewTimer});
handle_info({'DOWN', _MonitorRef, _Type, Pid, _Info}, State = #state{client = Pid, timer = Timer}) ->
   connection_registry:disconnected(),
   lager:warning("gun is down"),
   NewTimer = faxe_time:timer_cancel(Timer),
   handle_info(start_client, State#state{timer = NewTimer});
handle_info(start_client, State) ->
   NewState = start_client(State),
   {ok, NewState};
handle_info(_R, State) ->
   {ok, State}.

shutdown(#state{client = C}) ->
   gun:close(C).

try_request(Ts, State=#state{}) ->
   try_request(Ts, State, 0).

try_request(_Ts, State=#state{retries = Retries}, Retries) ->
   {ok, State};
try_request(Ts, State=#state{}, TriedSoFar) ->
   case request(State) of
      {ok, Data} ->
         {emit, build(Ts, Data, State), State};
      {failed, Reason} ->
         lager:warning("request failed with Reason: ~p, retry",[Reason]),
         try_request(Ts, State, TriedSoFar + 1);
      {error, Why} ->
         lager:warning("Error on request: ~p", [Why]),
         {ok, State}
   end.

request(#state{client = Client, path = Path}) ->
%%   Headers = [{<<"Accept">>, <<"application/json">>}, {<<"Cache-Control">>, {<<"max-age=0">>}}],
   %Headers = [{<<"Authorization">>, <<"Basic ">>}],
   StreamRef = gun:get(Client, Path, []),
   case gun:await(Client, StreamRef) of
      {response, fin, Status, _Headers} ->
         lager:notice("Response Status: ~p",[Status]),
         no_data;
      {response, nofin, Status, _Headers} ->
         {ok, Body} = gun:await_body(Client, StreamRef),
         handle_response(integer_to_binary(Status), Body)
   end.

build(Ts, Data, #state{payload_type = PType, as = As}) ->
   Content = case PType of ?P_TYPE_JSON -> jiffy:decode(Data, [return_maps]); _ -> Data end,
   case As of
      undefined -> #data_point{ts = Ts, fields = Content};
      _ -> #data_point{ts = Ts, fields = jsn:set(As, #{}, Content)}
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
         NewState = maybe_start_timer(State),
         NewState#state{client = C};
      E ->
         lager:warning("~p connecting to ~p:~p", [E, Host, Port]),
         erlang:demonitor(MonitorRef),
         erlang:send_after(1000, self(), start_client),
         State#state{client = undefined}
   end.


maybe_start_timer(State = #state{every = undefined}) ->
   State;
maybe_start_timer(State = #state{every = Every, align = Align}) ->
   Timer = faxe_time:init_timer(Align, Every, request),
   State#state{timer = Timer}.


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