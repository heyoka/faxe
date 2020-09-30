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
   init/2, allowed_methods/2, config_json/2, content_types_provided/2]).

%%
%% Additional callbacks
-export([
]).

-record(state, {mode}).

init(Req, [{op, Mode}]) ->
   {cowboy_rest, Req, #state{mode = Mode}}.

allowed_methods(Req, State) ->
    Value = [<<"GET">>, <<"OPTIONS">>],
    {Value, Req, State}.

content_types_provided(Req, State) ->
    {[
       {{<<"application">>, <<"json">>, []}, config_json}
    ], Req, State}.


%% faxe's config hand picked
config_json(Req, State=#state{mode = config}) ->
   Debug = opts_mqtt(debug),
   Metrics = opts_mqtt(metrics),
   ConnStatus = opts_mqtt(conn_status),
   DebugTime = faxe_config:get(debug_time),
   Out = #{<<"debug">> => Debug, <<"metrics">> => Metrics,
      <<"conn_status">> => ConnStatus, <<"debug_time_ms">> => DebugTime},
   {jiffy:encode(Out, [uescape]), Req, State}.

opts_mqtt(Key) ->
   [{handler, [{mqtt, Debug0}]}] = faxe_config:get(Key),
   All0 = faxe_util:proplists_merge(Debug0, faxe_config:get(mqtt, [])),
   All = lists:map(
      fun({K, V}) ->
         NewV =
         case K of
            base_topic ->
               Name = faxe_util:device_name(),
               T = rest_helper:to_bin(Key),
               faxe_util:build_topic([V, Name, T]);
%%               V0 = rest_helper:to_bin(V), T = rest_helper:to_bin(Key),
%%               filename:join(<<V0/binary>>, <<Name/binary, "/", T/binary>>);
            _ -> V
         end,
         {K, rest_helper:to_bin(NewV)}
      end,
      proplists:delete(ssl, All0)),
   maps:from_list(All).

