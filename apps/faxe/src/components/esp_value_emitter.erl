%% Date: 05.01.17 - 10:21
%% Ⓒ 2017 heyoka
-module(esp_value_emitter).
-author("Alexander Minichmair").

-behaviour(df_component).

-include("faxe.hrl").
%% API
-export([init/3, process/3, handle_info/2, options/0, params/0, handle_ack/2]).
-record(state, {
   node_id           :: term(),
   every             :: non_neg_integer(),
   jitter            :: non_neg_integer(),
   type              :: batch | point,
   format            :: undefined | ejson | json,
   batch_size        :: non_neg_integer(),
   align             :: atom(),
   fields            :: list(binary()),
   json_string       :: binary(),
   mode              :: binary()
}).

params() -> [].

options() ->
   [
      {every, duration, <<"5s">>},
      {jitter, duration, <<"0ms">>},
      {type, any, point},
      {batch_size, integer, 5},
      {align, is_set},
      {fields, binary_list, [<<"val">>]},
      {format, atom, undefined},
      {mode, string, <<"random">>}
   ].

init(NodeId, _Inputs,
    #{every := Every, type := Type, batch_size := BatchSize, align := Unit,
       fields := Fields, format := Fmt, jitter := Jitter, mode := Mode}) ->
   NUnit =
      case Unit of
         false -> false;
         true -> faxe_time:binary_to_duration(Every)
      end,
   JT = faxe_time:duration_to_ms(Jitter),
   EveryMs = faxe_time:duration_to_ms(Every),
   State = #state{node_id = NodeId, every = EveryMs, fields = Fields, mode = Mode,
      type = type(Type), batch_size = BatchSize, align = NUnit, format = Fmt, jitter = JT},

   erlang:send_after(EveryMs, self(), values),
   rand:seed(exs1024s),
   {ok, none, State}.


process(_Inport, _Value, State) ->
   {ok, State}.

handle_info(values, State=#state{every = Every,jitter = JT}) ->
   After = Every+(round(rand:uniform()*JT)),
   erlang:send_after(After, self(), values),
   Msg = build_msg(State),
%%   lager:info("~p emitting; ~p",[?MODULE, Msg]),
   {emit, {1, Msg}, State};
handle_info(_Request, State) ->
   {ok, State}.

handle_ack(DTag, State) ->
   lager:warning("got ack for Tag: ~p",[DTag]),
   {ok, State}.

build_msg(S = #state{type = batch, batch_size = Size}) ->
   {TsStart, Dist} = batch_start(S),
   Values = batch_points(TsStart, Dist, [], Size, S),
   flowdata:set_bounds(#data_batch{points = Values})
;
build_msg(S=#state{type = point, align = false, jitter = JT}) ->
   point(faxe_time:now()+(round(rand:uniform()*JT)), S);
build_msg(S=#state{type = point, align = Unit}) ->
%%   lager:info(" ~p build point~n",[?MODULE]),
   point(faxe_time:align(faxe_time:now(), Unit), S).

batch_start(#state{type = batch, batch_size = Size, every = Every, align = false}) ->
%%   lager:info("Align is undefined"),
   Ts0 = faxe_time:now() - ((Size + 1) * Every),
   {Ts0, Every};
batch_start(#state{type = batch, batch_size = Size, align = Unit}) ->
   Ts1 = faxe_time:now() - ((Size + 1) * faxe_time:unit_to_ms(Unit)),
   {faxe_time:align(Ts1, Unit), faxe_time:unit_to_ms(Unit)}.

batch_points(_Ts, _Dist, Vals, 0, #state{}) ->
   Vals;
batch_points(Ts, Dist, Vals, Num, S=#state{}) ->
   batch_points(Ts+Dist, Dist, [point(Ts, S)|Vals], Num - 1, S).

point(Ts, #state{fields = FieldNames, format = undefined, mode = Mode}) ->
   Fields =
   lists:foldl(
      fun(FName, FMap) ->
         FMap#{FName => val(Mode)}
      end,
      #{},
      FieldNames
   ),
   #data_point{ts = Ts, fields = Fields, dtag = count(dtag)};
point(Ts, #state{fields = FieldNames, format = ejson, mode = Mode}) ->
   Fields0 = #{},
   Fields =
      lists:foldl(
         fun(FName, FMap) ->
            #{<<FName/binary, <<"_root">>/binary >> => FMap#{FName => val(Mode)}}
         end,
         Fields0,
         FieldNames
      ),
      #data_point{ts = Ts, fields = Fields, dtag = count(dtag)}.

val(<<"random">>) -> rand:uniform()*10;
val(<<"monotonic_int">> = K) ->
   count(K).

count(K) ->
   Val = case get(K) of undefined -> 0; V -> V end,
   put(K, Val+1),
   Val.

type(<<"point">>) -> point;
type(<<"batch">>) -> batch;
type(point) -> point;
type(batch) -> batch;
type(_) -> point.