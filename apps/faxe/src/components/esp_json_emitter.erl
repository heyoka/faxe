%% Date: 05.01.17 - 10:21
%% Ⓒ 2017 heyoka
%% @todo implement align
-module(esp_json_emitter).
-author("Alexander Minichmair").

-behaviour(df_component).

-include("faxe.hrl").
%% API
-export([init/3, process/3, handle_info/2, options/0, params/0, check_options/0, check_json/1]).

-define(RAND, <<"rand">>).
-define(SEQ, <<"seq">>).

-record(state, {
   node_id           :: term(),
   every             :: non_neg_integer(),
   jitter            :: non_neg_integer(),
   align             :: atom(),
   json_string       :: binary(),
   ejson             :: map()|list(),
   as                :: binary(),
   select            :: binary(),
   idx = 1           :: non_neg_integer()
}).

params() -> [].

options() ->
   [
      {every, duration, <<"3s">>},
      {jitter, duration, <<"0ms">>},
      {align, is_set},
      {json, binary_list, undefined},
      {rand_fields, binary_list, []},
      {select, string, ?RAND}, %% 'rand' or 'seq'
      {as, string, <<"data">>}
   ].


check_options() ->
   [
      {func, json, fun check_json/1, <<", invalid json">>},
      {one_of, select, [?RAND, ?SEQ]}
   ].

check_json(Jsons) when is_list(Jsons) ->
   lists:all(fun check_json/1, Jsons);
check_json(Json) when is_binary(Json) ->
   case catch jiffy:decode(Json) of
      {'EXIT', _} -> false;
      _Other -> true
   end.

init(NodeId, _Inputs,
    #{every := Every, align := Unit, jitter := Jitter, json := JS, as := As, select := Sel}) ->
   NUnit =
      case Unit of
         false -> false;
         true -> faxe_time:binary_to_duration(Every)
      end,
   JT = faxe_time:duration_to_ms(Jitter),
   EveryMs = faxe_time:duration_to_ms(Every),
   JSONs = [jiffy:decode(JsonString, [return_maps]) || JsonString <- JS],
   State =
      #state{
         node_id = NodeId, every = EveryMs,
         ejson = JSONs, as = As, json_string = JS,
         align = NUnit, jitter = JT, select = Sel
      },

   erlang:send_after(JT, self(), emit),
   rand:seed(exs1024s),
   {ok, none, State}.


process(_Inport, _Value, State) ->
   {ok, State}.

handle_info(emit, State=#state{jitter = 0, every = Every}) ->
   do_emit(Every, State);
handle_info(emit, State=#state{every = Every, jitter = JT}) ->
   Jitter = round(rand:uniform()*JT),
   After = Every+(Jitter),
   do_emit(After, State);
handle_info(_Request, State) ->
   {ok, State}.

do_emit(Next, State=#state{ejson = JS, as = As}) ->
   erlang:send_after(Next, self(), emit),
   {NextIndex, NewState} = next_index(State),
   Json = lists:nth(NextIndex, JS),
   Msg = build(Json, As),
   {emit,{1, Msg}, NewState}.

next_index(S = #state{select = ?RAND, ejson = JS}) ->
   {rand:uniform(length(JS)), S};
next_index(S = #state{select = ?SEQ, ejson = JS, idx = Index}) when Index > length(JS) ->
   {1, S#state{idx = 2}};
next_index(S = #state{select = ?SEQ, ejson = _JS, idx = Index}) ->
   {Index, S#state{idx = Index+1}}.

build(JsonMap, As) when is_map(JsonMap) ->
   flowdata:set_root(#data_point{ts = faxe_time:now(), fields = JsonMap}, As);
build(JsonList, As) when is_list(JsonList) ->
   Points = [build(JsonMap, As) || JsonMap <- JsonList],
   #data_batch{points = Points}.



