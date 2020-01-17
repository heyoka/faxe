%% Date: 05.01.17 - 10:21
%% Ⓒ 2017 heyoka
-module(esp_json_emitter).
-author("Alexander Minichmair").

-behaviour(df_component).

-include("faxe.hrl").
%% API
-export([init/3, process/3, handle_info/2, options/0, params/0]).
-record(state, {
   node_id           :: term(),
   every             :: non_neg_integer(),
   jitter            :: non_neg_integer(),
   align             :: atom(),
   json_string       :: binary(),
   ejson :: map()|list()
}).

params() -> [].

options() ->
   [{every, duration, <<"3s">>},
      {jitter, duration, <<"0ms">>},
      {align, is_set},
      {json, binary_list, undefined},
      {rand_fields, binary_list, []}].

init(NodeId, _Inputs,
    #{every := Every, align := Unit, jitter := Jitter, json := JS}) ->
   NUnit =
      case Unit of
         false -> false;
         true -> faxe_time:binary_to_duration(Every)
      end,
   JT = faxe_time:duration_to_ms(Jitter),
   EveryMs = faxe_time:duration_to_ms(Every),
   JSONs = [jiffy:decode(JsonString, [return_maps]) || JsonString <- JS],
   State = #state{node_id = NodeId, every = EveryMs, ejson = JSONs,
      json_string = JS, align = NUnit, jitter = JT},

   erlang:send_after(JT, self(), emit),
   rand:seed(exs1024s),
   {ok, none, State}.


process(_Inport, _Value, State) ->
   {ok, State}.

handle_info(emit, State=#state{every = Every, jitter = JT, ejson = JS}) ->
   Jitter = round(rand:uniform()*JT),
   After = Every+(Jitter),
   erlang:send_after(After, self(), emit),
   JsonMap = lists:nth(rand:uniform(length(JS)), JS),
   Msg = #data_point{ts = faxe_time:now(), fields = #{<<"data">> => JsonMap}},
   dataflow:emit(Msg),
   {ok, State};
handle_info(_Request, State) ->
   {ok, State}.

