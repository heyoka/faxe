%% Date: 05.01.17 - 10:21
%% Ⓒ 2017 heyoka
%% @todo implement align
-module(esp_json_emitter).
-author("Alexander Minichmair").

-behaviour(df_component).

-include("faxe.hrl").
%% API
-export([init/3, process/3, handle_info/2, options/0, check_options/0, check_json/1]).

-define(RAND, <<"rand">>).
-define(SEQ, <<"seq">>).

-define(STATE_POINT_FIELD, <<"__state">>).

-record(state, {
   node_id           :: term(),
   every             :: non_neg_integer(),
   jitter            :: non_neg_integer(),
   align             :: atom(),
   json_string       :: binary(),
   ejson             :: map()|list(),
   as                :: binary(),
   select            :: binary(),
   transforms        :: list()|undefined,
   state_point       :: #data_point{}|undefined,
   idx = 1           :: non_neg_integer()
}).


options() ->
   [
      #{name => every, type => duration, default => <<"3s">>,
         desc => <<"emit interval">>},
      #{name => jitter, type => duration, default => <<"0ms">>,
         desc => <<"max random value for added time jitter added to every">>},
      #{name => align, type => is_set, default => false,
         desc => <<"whether to align to full occurencies of the every parameter">>},
      #{name => json, type => string_list,
         desc => <<"list of json strings to use">>},
      #{name => select, type => string, default => ?RAND, values => [?RAND, ?SEQ],
         desc => <<"how to select the json entries, 'rand' or 'seq'">>},
      #{name => modify, type => string_list, default => undefined,
         desc => <<"fields to replace on every interval">>},
      #{name => modify_with, type => lambda_list, default => undefined,
         desc => <<"lambda expressions for the replacements">>},
      #{name => as, type => string, default => <<"data">>,
         desc => <<"root object for output">>}
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
   end;
check_json(_) -> false.

init(NodeId, _Inputs,
    #{every := Every, align := Unit, jitter := Jitter, json := JS, as := As, select := Sel, modify := Replace,
       modify_with := Funs0} = Opts) ->
   lager:notice("~n~p got options: ~n~p", [?MODULE, Opts]),
   NUnit =
      case Unit of
         false -> false;
         true -> faxe_time:binary_to_duration(Every)
      end,
   JT = faxe_time:duration_to_ms(Jitter),
   EveryMs = faxe_time:duration_to_ms(Every),

   JSONs = [jiffy:decode(JsonString, [return_maps]) || JsonString <- JS],
   TransformList = case Replace of undefined -> undefined; _ -> lists:zip(Replace, Funs0) end,
   State =
      #state{
         node_id = NodeId, every = EveryMs,
         ejson = JSONs, as = As, json_string = JS,
         align = NUnit, jitter = JT, select = Sel,
         transforms = TransformList
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

do_emit(Next, State=#state{ejson = JS, as = As, transforms = Transforms, state_point = SPoint0}) ->
   erlang:send_after(Next, self(), emit),
   {NextIndex, NewState} = next_index(State),
   Json = lists:nth(NextIndex, JS),
   SPoint =
   case SPoint0 of
      undefined -> flowdata:set_root(#data_point{ts = faxe_time:now(), fields = Json}, As);
      _ -> SPoint0
   end,
   Msg = build(Json, As, Transforms, SPoint),
%%   {T, Msg} = timer:tc(fun build/4, [Json, As, Transforms, SPoint]),
%%   lager:info("time to build: ~p",[T]),
   {emit,{1, Msg}, NewState#state{state_point = Msg}}.

next_index(S = #state{select = ?RAND, ejson = JS}) ->
   {rand:uniform(length(JS)), S};
next_index(S = #state{select = ?SEQ, ejson = JS, idx = Index}) when Index > length(JS) ->
   {1, S#state{idx = 2}};
next_index(S = #state{select = ?SEQ, ejson = _JS, idx = Index}) ->
   {Index, S#state{idx = Index+1}}.

build(JsonMap, As, Transforms, SPoint) when is_map(JsonMap) ->
   Point =
    flowdata:set_root(#data_point{ts = faxe_time:now(), fields = JsonMap}, As),
   maybe_transform(Point, Transforms, SPoint);
build(JsonList, As, Transforms, SPoint) when is_list(JsonList) ->
   Points = [build(JsonMap, As, Transforms, SPoint) || JsonMap <- JsonList],
   #data_batch{points = Points}.

maybe_transform(Point, undefined, _SP) ->
   Point;
maybe_transform(Point, ReplaceList, SPoint) ->
   do_transform(ReplaceList, flowdata:set_field(Point, ?STATE_POINT_FIELD, SPoint#data_point.fields)).

do_transform([], Point) ->
   flowdata:delete_field(Point, ?STATE_POINT_FIELD);
do_transform([{FieldPath, Lambda}|Transforms], Point) ->
   NewPoint = faxe_lambda:execute(Point, Lambda, FieldPath),
   do_transform(Transforms, NewPoint).




