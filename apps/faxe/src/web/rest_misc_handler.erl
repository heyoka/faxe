%%%-------------------------------------------------------------------
%%% @author Alexander Minichmair
%%% @copyright (C) 2020
%%% @doc
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(rest_misc_handler).

%%
%% Cowboy callbacks
-export([
   init/2, allowed_methods/2, config_json/2, content_types_provided/2,
   is_authorized/2, content_types_accepted/2, from_validate_dfs/2,
   malformed_request/2, from_set_loglevel/2, loglevels_json/2, config_all_json/2, language_definition_json/2]).

%%
%% Additional callbacks
-export([
]).

-record(state, {mode, dfs, level, backend}).

init(Req, [{op, Mode}]) ->
   {cowboy_rest, Req, #state{mode = Mode}}.

is_authorized(Req, State) ->
   rest_helper:is_authorized(Req, State).

allowed_methods(Req, S=#state{mode = loglevels}) ->
   {[<<"GET">>], Req, S};
allowed_methods(Req, S=#state{mode = language_definition}) ->
   {[<<"GET">>], Req, S};
allowed_methods(Req, S=#state{mode = loglevel}) ->
   {[<<"POST">>], Req, S};
allowed_methods(Req, S=#state{mode = validate_dfs}) ->
   {[<<"POST">>], Req, S};
allowed_methods(Req, State) ->
    Value = [<<"GET">>],
    {Value, Req, State}.

content_types_accepted(Req = #{method := <<"POST">>}, State = #state{mode = validate_dfs}) ->
   Value = [{{ <<"application">>, <<"x-www-form-urlencoded">>, []}, from_validate_dfs}],
   {Value, Req, State};
content_types_accepted(Req = #{method := <<"POST">>}, State = #state{mode = loglevel}) ->
   Value = [{{ <<"application">>, <<"x-www-form-urlencoded">>, []}, from_set_loglevel}],
   {Value, Req, State}.


content_types_provided(Req, State = #state{mode = config}) ->
    {[
       {{<<"application">>, <<"json">>, []}, config_json}
    ], Req, State};
content_types_provided(Req, State = #state{mode = language_definition}) ->
   {[
      {{<<"application">>, <<"json">>, []}, language_definition_json}
   ], Req, State};
content_types_provided(Req, State = #state{mode = config_all}) ->
   {[
      {{<<"text">>, <<"plain">>, []}, config_all_json}
   ], Req, State};
content_types_provided(Req = #{method := <<"GET">>}, State = #state{mode = loglevels}) ->
   {[
      {{<<"application">>, <<"json">>, []}, loglevels_json}
   ], Req, State};
content_types_provided(Req, State) ->
   {
      [{{ <<"application">>, <<"json">>, '*'}, to_json}
      ], Req, State}.


malformed_request(Req, State=#state{mode = validate_dfs}) ->
   {ok, Result, Req1} = cowboy_req:read_urlencoded_body(Req),
   Dfs = proplists:get_value(<<"dfs">>, Result, undefined),
   Malformed = Dfs == undefined,
   {Malformed, rest_helper:report_malformed(Malformed, Req1, [<<"dfs">>]),
      State#state{dfs = Dfs}};
malformed_request(Req = #{method := <<"POST">>}, State=#state{mode = loglevel}) ->
   {ok, Result, Req1} = cowboy_req:read_urlencoded_body(Req),
   Backend = cowboy_req:binding(backend, Req),
   BackendList = log_backends(),
   {_Modules, Backends} = lists:unzip(BackendList),
   case lists:member(Backend, Backends) of
      true ->
         Level = proplists:get_value(<<"level">>, Result, undefined),
         case Level of
            undefined ->
               {true, rest_helper:report_malformed(true, Req1, [<<"level">>]), State};
            _ ->
               case valid_level(Level) of
                  true -> {false, Req1, State#state{level = Level, backend = Backend}};
                  false ->
                     Req2 = cowboy_req:set_resp_header(<<"Content-Type">>, <<"application/json">>, Req),
                     Req3 = cowboy_req:set_resp_body(jiffy:encode(#{success => false,
                        <<"param_invalid">> => <<"level">>, <<"possible_values">> => levels()}), Req2),
                     {true, Req3, State}
               end
         end;
      false ->
         Req2 = cowboy_req:set_resp_header(<<"Content-Type">>, <<"application/json">>, Req),
         Req3 = cowboy_req:set_resp_body(jiffy:encode(#{success => false,
            <<"param_invalid">> => <<"backend">>, <<"possible_values">> => Backends}), Req2),
         {true, Req3, State}
   end;
malformed_request(Req, State) ->
   {false, Req, State}.

from_set_loglevel(Req, State = #state{level = Level, backend = Backend}) ->
   {Mods, Names} = lists:unzip(log_backends()),
   ModBackends = lists:zip(Names, Mods),
   BackendModule = proplists:get_value(Backend, ModBackends),
   ok = lager:set_loglevel(BackendModule, binary_to_existing_atom(Level, utf8)),
   Response = #{<<"success">> => true,
      <<"message">> => <<"Log level for '", Backend/binary, "' is now '", Level/binary, "'">>},
   Req2 = cowboy_req:set_resp_body(jiffy:encode(Response), Req),
   {true, Req2, State}.

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

loglevels_json(Req, State=#state{}) ->
   Out = [{Name, lager:get_loglevel(Module)} || {Module, Name} <- log_backends()],
   {jiffy:encode(maps:from_list(Out), [uescape]), Req, State}.

language_definition_json(Req, State=#state{}) ->
   {jiffy:encode(graph_builder:node_opts(), [uescape]), Req, State}.
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

config_all_json(Req, State) ->
   ConfigAll = application:get_all_env(faxe),
   CString = io_lib:format("~p", [ConfigAll]),
%%   Req2 = cowboy_req:set_resp_body(ConfigAll, Req),
%%   Req3 = cowboy_req:set_resp_header(<<"content-type">>)
%%   {true, Req2, State}.
   {CString, Req, State}.

opts_mqtt(Key, TopicKey) ->
   Debug0 = get_config_mqtt_handler(Key),
   ReportMqttHost = proplists:get_value(mqtt_host, faxe_config:get(report_debug)),
   All0 = faxe_event_handlers:mqtt_opts(Debug0),
   All = lists:map(
      fun({K, V}) ->
         NewV =
         case K of
            base_topic ->
               Name = faxe_util:device_name(),
               faxe_util:build_topic([V, Name, TopicKey]);
            host when ReportMqttHost =/= [] -> ReportMqttHost;
            _ -> V
         end,
         {K, faxe_util:to_bin(NewV)}
      end,
      proplists:delete(ssl, All0)),
   maps:without([user, pass], maps:from_list(All)).

get_config_mqtt_handler(Key) ->
   C0 = faxe_config:get(Key),
   case proplists:get_value(handler, C0) of
      undefined -> [];
      [{mqtt, Mqtt}] -> Mqtt
   end.

valid_level(Level) ->
   lists:member(Level, levels()).

levels() ->
   [atom_to_binary(L, utf8) || L <- lager_util:levels()].


log_backends() ->
   [{Module, log_backend(Module)} || Module <- lager_handlers(), Module /= lager_file_backend].

log_backend(lager_logstash_backend) -> <<"logstash">>;
log_backend(lager_console_backend) -> <<"console">>;
log_backend(lager_file_backend) -> <<"file">>;
log_backend(lager_emit_backend) -> <<"emit">>.

lager_handlers() ->
   AllConf = application:get_all_env(lager),
   Handlers = proplists:get_value(handlers, AllConf),
   proplists:get_keys(Handlers).