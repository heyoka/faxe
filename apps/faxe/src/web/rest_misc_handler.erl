%%%-------------------------------------------------------------------
%%% @author $author
%%% @copyright (C) $year, $company
%%% @doc
%%%
%%% @end
%%% Created : $fulldate
%%%-------------------------------------------------------------------
-module(rest_misc_handler).

%%
%% Cowboy callbacks
-export([
   init/2, allowed_methods/2, config_json/2, content_types_provided/2, is_authorized/2, content_types_accepted/2, from_validate_dfs/2, malformed_request/2]).

%%
%% Additional callbacks
-export([
]).

-record(state, {mode, dfs}).

init(Req, [{op, Mode}]) ->
   {cowboy_rest, Req, #state{mode = Mode}}.

is_authorized(Req, State) ->
   rest_helper:is_authorized(Req, State).

allowed_methods(Req, S=#state{mode = validate_dfs}) ->
   {[<<"POST">>], Req, S};
allowed_methods(Req, State) ->
    Value = [<<"GET">>, <<"OPTIONS">>],
    {Value, Req, State}.

content_types_accepted(Req = #{method := <<"POST">>}, State = #state{mode = validate_dfs}) ->
   Value = [{{ <<"application">>, <<"x-www-form-urlencoded">>, []}, from_validate_dfs}],
   {Value, Req, State}.


content_types_provided(Req, State) ->
    {[
       {{<<"application">>, <<"json">>, []}, config_json}
    ], Req, State}.

malformed_request(Req, State=#state{mode = validate_dfs}) ->
   {ok, Result, Req1} = cowboy_req:read_urlencoded_body(Req),
   Dfs = proplists:get_value(<<"dfs">>, Result, undefined),
   Malformed = Dfs == undefined,
   {Malformed, rest_helper:report_malformed(Malformed, Req1, [<<"dfs">>]),
      State#state{dfs = Dfs}};
malformed_request(Req, State) ->
   {false, Req, State}.

from_validate_dfs(Req, State = #state{dfs = DfsScript}) ->
   Response =
   case faxe:eval_dfs(DfsScript, data) of
      {_DFS, Def} = _Res when is_map(Def) ->
         #{<<"success">> => true, <<"message">> => <<"Dfs is valid.">>};
      {error, What} ->
         #{<<"success">> => false, <<"message">> => faxe_util:to_bin(What)}
   end,
   Req2 = cowboy_req:set_resp_body(jiffy:encode(Response), Req),
   {true, Req2, State}.


%% faxe's config hand picked
config_json(Req, State=#state{mode = config}) ->
   Debug = opts_mqtt(debug, <<"debug">>),
   Logs = opts_mqtt(debug, <<"log">>),
   Metrics = opts_mqtt(metrics, <<"metrics">>),
   ConnStatus = opts_mqtt(conn_status, <<"conn_status">>),
   DebugTime = faxe_config:get(debug_time),
   Out = #{<<"debug">> => Debug, <<"log">> => Logs, <<"metrics">> => Metrics,
      <<"conn_status">> => ConnStatus, <<"debug_time_ms">> => DebugTime},
   {jiffy:encode(Out, [uescape]), Req, State}.

opts_mqtt(Key, TopicKey) ->
   [{handler, [{mqtt, Debug0}]}] = faxe_config:get(Key),
   All0 = faxe_event_handlers:mqtt_opts(Debug0),
   All = lists:map(
      fun({K, V}) ->
         NewV =
         case K of
            base_topic ->
               Name = faxe_util:device_name(),
               faxe_util:build_topic([V, Name, TopicKey]);
            _ -> V
         end,
         {K, faxe_util:to_bin(NewV)}
      end,
      proplists:delete(ssl, All0)),
   maps:without([user, pass], maps:from_list(All)).

