%% Date: 30.08.21 - 16:44
%% HTTP Listen
%% Ⓒ 2021 heyoka
%%
%%
-module(esp_http_listen).
-author("Alexander Minichmair").

-include("faxe.hrl").

-behavior(df_component).
%% API
-export([init/3, process/3, options/0, handle_info/2, check_options/0, metrics/0]).

-define(CONT_TYPE_PLAIN, <<"text/plain">>).
-define(CONT_TYPE_FORM_URLENCODED, <<"application/x-www-form-urlencoded">>).
-define(CONT_TYPE_JSON, <<"application/json">>).
-define(P_TYPE_PLAIN, <<"plain">>).
-define(P_TYPE_JSON, <<"json">>).

-record(state, {
   port :: non_neg_integer(),
   tls = false :: true|false,
   payload_type = <<"plain">> :: binary(),
   as,
   fn_id,
   cowboy_id :: tuple()
}).

options() ->
   [
      {port, integer, 8899},
      {path, string, <<"/">>},
      {tls, is_set, false},
      {payload_type, string, ?P_TYPE_JSON},
      {content_type, string, ?CONT_TYPE_JSON},
      {as, string, undefined},
      {user, string, undefined},
      {pass, string, undefined}
   ].

check_options() ->
   [
      {one_of, content_type, [?CONT_TYPE_FORM_URLENCODED, ?CONT_TYPE_PLAIN, ?CONT_TYPE_JSON]},
      {one_of, payload_type, [?P_TYPE_PLAIN, ?P_TYPE_JSON]},
      {func, pass,
         fun(Pass, #{user := User}) ->
            case User of
               undefined -> true;
               Other when is_binary(Other) -> Pass /= undefined
            end
         end,
         <<" must be given, if 'user' is given.">>
      }
   ].

metrics() ->
   [
      {?METRIC_BYTES_READ, meter, []}
   ].

init(NodeId, _Inputs,
    #{port := Port, path := Path0, tls := Tls, payload_type := PType, content_type := CType, as := As,
       user := User, pass := Pass}) ->

   Path = binary_to_list(Path0),
   Alias =
   case {As, CType} of
      {undefined, TypeV} when TypeV == ?CONT_TYPE_JSON orelse TypeV == ?CONT_TYPE_PLAIN -> <<"data">>;
      _ -> As
   end,
   [Type, SubType] = binary:split(CType, <<"/">>),
   ContentType = {Type, SubType, []},
   CowboyOpts = #{path => Path, port => Port, tls => Tls, content_type => ContentType, user => User, pass => Pass},
   case http_manager:reg(CowboyOpts) of
      ok -> {ok, all,
         #state{port = Port, tls = Tls, fn_id = NodeId, payload_type = PType, as = Alias}};
      {error, Reason} ->
         {error, Reason}
   end.

process(_In, #data_point{}, State = #state{}) ->
   {ok, State};
process(_In, #data_batch{}, State = #state{}) ->
   {ok, State}.

handle_info({http_data, Data, Body_Length}, State = #state{payload_type = PType}) when is_binary(Data) ->
   Content = case PType of ?P_TYPE_JSON -> jiffy:decode(Data, [return_maps]); _ -> Data end,
   emit(Content, Body_Length, State);
handle_info({http_data, Data, Body_Length}, State = #state{payload_type = PType}) when is_list(Data) ->
   Fields0 = maps:from_list(Data),
   Fields1 =
   case PType of
      ?P_TYPE_JSON -> maps:map(fun(_K, V) -> jiffy:decode(V, [return_maps]) end, Fields0);
      ?P_TYPE_PLAIN -> Fields0
   end,
   emit(Fields1, Body_Length, State);
handle_info(_R, State) ->
   {ok, State}.

emit(CFields, Body_Length, S = #state{as = As, fn_id = FNId}) ->
   node_metrics:metric(?METRIC_BYTES_READ, Body_Length, FNId),
   node_metrics:metric(?METRIC_ITEMS_IN, 1, FNId),
   Fields =
      case As of
         undefined -> CFields;
         V when is_binary(V) -> jsn:set(As, #{}, CFields)
      end,
   P = #data_point{ts = faxe_time:now(), fields = Fields},
   {emit, P, S}.