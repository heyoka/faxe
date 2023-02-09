%% Date: 05.01.17 - 10:21
%% Ⓒ 2017 heyoka
-module(esp_json_emitter).
-author("Alexander Minichmair").

-behaviour(df_component).

-include("faxe.hrl").
%% API
-export([init/3, process/3, handle_info/2, options/0, check_options/0, check_json/1]).

-define(RAND, <<"rand">>).
-define(SEQ, <<"seq">>).
-define(BATCH, <<"batch">>).

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
   idx = 1           :: non_neg_integer(),
   current_ts        :: non_neg_integer()|undefined,
   start_ts          :: non_neg_integer()|undefined,
   one_shot = false  :: true|false
}).


options() ->
   [
      #{name => every, type => duration, default => <<"3s">>,
         desc => <<"emit interval">>},
      #{name => jitter, type => duration, default => <<"0ms">>,
         desc => <<"max random value for added time jitter added to every">>},
      #{name => align, type => bool, default => false,
         desc => <<"whether to align to full occurencies of the every parameter">>},
      #{name => json, type => string_list,
         desc => <<"list of json strings to use">>},
      #{name => select, type => string, default => ?RAND, values => [?RAND, ?SEQ, ?BATCH],
         desc => <<"how to select the json entries, 'rand', 'seq' or 'batch'">>},
      #{name => modify, type => string_list, default => undefined,
         desc => <<"fields to replace on every interval">>},
      #{name => modify_with, type => lambda_list, default => undefined,
         desc => <<"lambda expressions for the replacements">>},
      #{name => one_shot, type => boolean, default => false,
         desc => <<"one-shot mode">>},
      #{name => start_ts, type => any, default => undefined,
         desc =>
         <<"provide a timestamp, that will be used to base produced message timestamps on, instead of wall-clock timer">>},
      #{name => as, type => string, default => <<>>,
         desc => <<"root object for output">>}
   ].


check_options() ->
   [
      {func, json, fun check_json/1, <<", invalid json">>},
      {one_of, select, [?RAND, ?SEQ, ?BATCH]},
      {func, start_ts, fun check_start_ts/1, <<", ms timestamp (integer) or ISO8601 format expected">>}
   ].

check_json(Jsons) when is_list(Jsons) ->
   lists:all(fun check_json/1, Jsons);
check_json(Json) when is_binary(Json) ->
   case catch jiffy:decode(Json) of
      {'EXIT', _} -> false;
      _Other -> true
   end;
check_json(_) -> false.

check_start_ts(undefined) ->
   true;
check_start_ts(Start) when is_binary(Start) ->
   case catch time_format:iso8601_to_ms(Start) of
      Ts when is_integer(Ts) -> true;
      _ -> false
   end;
check_start_ts(Start) ->
   faxe_time:is_timestamp(Start).


init(NodeId, _Inputs,
    #{every := Every, align := Unit, jitter := Jitter, json := JS, as := As, select := Sel, modify := Replace,
       modify_with := Funs0, one_shot := OneShot, start_ts := StartTs} = _Opts) ->

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
         transforms = TransformList, one_shot = OneShot,
         start_ts = StartTs
      },

%%   case Sel of
%%      <<"batch">> -> lager:notice("select batch gives batch-interval of ~p ms (round(~p/~p)",
%%         [round(EveryMs/length(JSONs)), {Every, EveryMs}, length(JSONs)]);
%%      _ -> ok
%%   end,

   erlang:send_after(JT, self(), emit),
   rand:seed(exs1024s),
   {ok, none, init_ts(State)}.


process(_Inport, _Value, State) ->
   {ok, State}.

handle_info(emit, State=#state{jitter = 0, every = Every}) ->
   select_emit(Every, State);
handle_info(emit, State=#state{every = Every, jitter = JT}) ->
   Jitter = round(rand:uniform()*JT),
   After = Every+(Jitter),
   select_emit(After, State);
handle_info(_Request, State) ->
   {ok, State}.

select_emit(Next, State=#state{select = ?BATCH, ejson = JS, as = As, transforms = Transforms, current_ts = Ts}) ->
   SPoint = get_state_point(State, JS),
   Batch = build(JS, Ts, As, Transforms, SPoint),
   do_emit(Next, Batch, State);
select_emit(Next, State=#state{ejson = JS, as = As, transforms = Transforms, current_ts = Ts}) ->
   {NextIndex, NewState} = next_index(State),
   Json = lists:nth(NextIndex, JS),
   SPoint = get_state_point(State, Json),
   Item = build(Json, Ts, As, Transforms, SPoint),
   do_emit(Next, Item, NewState).
do_emit(Next, Msg, State=#state{one_shot = true, current_ts = CTs}) ->
   {emit,{1, Msg}, State#state{state_point = state_point(Msg), current_ts = CTs + Next}};
do_emit(Next, Msg, State=#state{current_ts = CTs}) ->
   erlang:send_after(Next, self(), emit),
   {emit,{1, Msg}, State#state{state_point = state_point(Msg), current_ts = CTs + Next}}.

next_index(S = #state{select = ?BATCH, ejson = _JS}) ->
   {1, S#state{idx = 1}};
next_index(S = #state{select = ?RAND, ejson = JS}) ->
   {rand:uniform(length(JS)), S};
next_index(S = #state{select = ?SEQ, ejson = JS, idx = Index}) when Index > length(JS) ->
   {1, S#state{idx = 2}};
next_index(S = #state{select = ?SEQ, ejson = _JS, idx = Index}) ->
   {Index, S#state{idx = Index+1}}.

build(JsonMap, Ts, As, Transforms, SPoint) when is_map(JsonMap) ->
   Point =
    flowdata:set_root(#data_point{ts = Ts, fields = JsonMap}, As),
   maybe_transform(Point, Transforms, SPoint);
build(JsonList, Ts, As, Transforms, SPoint) when is_list(JsonList) ->
   Points = [build(JsonMap, Ts, As, Transforms, SPoint) || JsonMap <- JsonList],
   #data_batch{points = Points, start = Ts}.

maybe_transform(Point, undefined, _SP) ->
   Point;
maybe_transform(Point, ReplaceList, SPoint) when is_record(SPoint, data_point) ->
   do_transform(ReplaceList, flowdata:set_field(Point, ?STATE_POINT_FIELD, SPoint#data_point.fields));
maybe_transform(Point, ReplaceList, _SPoint) ->
   do_transform(ReplaceList, Point).

do_transform([], Point) ->
   flowdata:delete_field(Point, ?STATE_POINT_FIELD);
do_transform([{FieldPath, Lambda}|Transforms], Point) ->

%%   lager:info("do_transform ~p on ~p",[Point, FieldPath]),
   NewPoint = faxe_lambda:execute(Point, Lambda, FieldPath),
   do_transform(Transforms, NewPoint).

get_state_point(S = #state{state_point = undefined}, [First|_]) ->
   get_state_point(S, First);
get_state_point(#state{state_point = undefined, current_ts = CTs, as = As}, InitialData) ->
   flowdata:set_root(#data_point{ts = CTs, fields = InitialData}, As);
get_state_point(#state{state_point = SPoint}, _) ->
   SPoint.

state_point(P = #data_point{}) ->
   P;
state_point(#data_batch{points = [P|_]}) ->
   P.

init_ts(S = #state{start_ts = undefined, align = false}) ->
   S#state{current_ts = faxe_time:now()};
init_ts(S = #state{start_ts = undefined, align = Align}) ->
   S#state{current_ts = faxe_time:align(faxe_time:now(), Align)};
init_ts(S = #state{start_ts = Start}) when is_binary(Start)->
   S#state{current_ts = time_format:iso8601_to_ms(Start)};
init_ts(S = #state{start_ts = Start}) when is_integer(Start)->
   S#state{current_ts = Start}.





