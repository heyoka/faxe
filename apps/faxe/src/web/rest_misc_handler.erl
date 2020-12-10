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
   init/2, allowed_methods/2, config_json/2, content_types_provided/2, is_authorized/2]).

%%
%% Additional callbacks
-export([
]).

-record(state, {mode}).

init(Req, [{op, Mode}]) ->
   {cowboy_rest, Req, #state{mode = Mode}}.

is_authorized(Req, State) ->
   rest_helper:is_authorized(Req, State).

allowed_methods(Req, State) ->
    Value = [<<"GET">>, <<"OPTIONS">>],
    {Value, Req, State}.

content_types_provided(Req, State) ->
    {[
       {{<<"application">>, <<"json">>, []}, config_json}
    ], Req, State}.


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

