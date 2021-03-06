%% Date: 05.01.17 - 10:21
%% Ⓒ 2017 heyoka
-module(esp_value_emitter).
-author("Alexander Minichmair").

-behaviour(df_component).

-include("faxe.hrl").
%% API
-export([init/3, process/3, handle_info/2, options/0, params/0]).
-record(state, {
   node_id           :: term(),
   every             :: non_neg_integer(),
   jitter            :: non_neg_integer(),
   type              :: batch | point,
   format            :: undefined | ejson | json,
   batch_size        :: non_neg_integer(),
   align             :: atom(),
   fields            :: list(binary()),
   json_string       :: binary()
}).

params() -> [].

options() ->
   [{every, duration, <<"5s">>}, {jitter, duration, <<"0ms">>}, {type, atom, batch},
      {batch_size, integer, 5}, {align, is_set},
      {fields, binary_list, [<<"val">>]}, {format, atom, undefined}].

init(NodeId, _Inputs,
    #{every := Every, type := Type, batch_size := BatchSize, align := Unit,
       fields := Fields, format := Fmt, jitter := Jitter}) ->
   NUnit =
      case Unit of
         false -> false;
         true -> faxe_time:binary_to_duration(Every)
      end,
   JT = faxe_time:duration_to_ms(Jitter),
   EveryMs = faxe_time:duration_to_ms(Every),
   State = #state{node_id = NodeId, every = EveryMs, fields = Fields,
      type = Type, batch_size = BatchSize, align = NUnit, format = Fmt, jitter = JT},

   erlang:send_after(EveryMs, self(), values),
   rand:seed(exs1024s),
%%   lager:info("~p state is : ~p",[?MODULE, State]),
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

build_msg(S = #state{type = batch, batch_size = Size, fields = Fields, format = Fmt}) ->
   {TsStart, Dist} = batch_start(S),
   Values = batch_points(TsStart, Dist, [], Size, Fields, Fmt),
   flowdata:set_bounds(#data_batch{points = Values})
;
build_msg(#state{type = point, align = false, fields = Fields, format = Fmt, jitter = JT}) ->
   point(faxe_time:now()+(round(rand:uniform()*JT)), Fields, Fmt);
build_msg(#state{type = point, align = Unit, fields = Fields, format = Fmt}) ->
%%   lager:info(" ~p build point~n",[?MODULE]),
   point(faxe_time:align(faxe_time:now(), Unit), Fields, Fmt).

batch_start(#state{type = batch, batch_size = Size, every = Every, align = false}) ->
%%   lager:info("Align is undefined"),
   Ts0 = faxe_time:now() - ((Size + 1) * Every),
   {Ts0, Every};
batch_start(#state{type = batch, batch_size = Size, align = Unit}) ->
   Ts1 = faxe_time:now() - ((Size + 1) * faxe_time:unit_to_ms(Unit)),
   {faxe_time:align(Ts1, Unit), faxe_time:unit_to_ms(Unit)}.

batch_points(_Ts, _Dist, Vals, 0, _F, _Fmt) ->
   Vals;
batch_points(Ts, Dist, Vals, Num, Fields, Format) ->
   batch_points(Ts+Dist, Dist, [point(Ts, Fields, Format)|Vals], Num - 1, Fields, Format).

point(Ts, FieldNames, undefined) ->
   Fields0 = #{},
   Fields =
   lists:foldl(
      fun(FName, FMap) ->
         FMap#{FName => rand:uniform()*10}
      end,
      Fields0,
      FieldNames
   ),
   #data_point{ts = Ts, fields = Fields};
point(Ts, FieldNames, ejson) ->
   Fields0 = #{},
   Fields =
      lists:foldl(
         fun(FName, FMap) ->
            #{<<FName/binary, <<"_root">>/binary >> => FMap#{FName => rand:uniform()*10}}
         end,
         Fields0,
         FieldNames
      ),
      #data_point{ts = Ts, fields = Fields}.