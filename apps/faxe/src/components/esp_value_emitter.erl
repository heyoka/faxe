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
   type              :: batch | point,
   batch_size        :: non_neg_integer(),
   align             :: atom()
}).

params() -> [].

options() ->
   [{every, binary, <<"5s">>}, {type, atom, batch}, {batch_size, integer, 5}, {align, is_set}].

init(NodeId, _Inputs, #{every := Every, type := Type, batch_size := BatchSize, align := Unit} = P) ->
   NUnit =
      case Unit of
         false -> false;
         true -> faxe_time:binary_to_duration(Every)
      end,

   lager:debug("~p init:node~n",[{NodeId, P}]),
   EveryMs = faxe_time:duration_to_ms(Every),
   State = #state{node_id = NodeId, every = EveryMs,
      type = Type, batch_size = BatchSize, align = NUnit},

   erlang:send_after(EveryMs, self(), values),
   random:seed(),
   lager:info("~p state is : ~p",[?MODULE, State]),
   {ok, none, State}.


process(_Inport, Value, State) ->
   io:format("~p process, ~p~n",[State, {_Inport, Value}]),
   {ok, State}.

handle_info(values, State=#state{every = Every}) ->
   erlang:send_after(Every, self(), values),
   Msg = build_msg(State),
   lager:debug("~p emitting; ~p",[?MODULE, Msg]),
   dataflow:emit(Msg),
   {ok, State};
handle_info(Request, State) ->
   lager:debug("~p request: ~p~n", [State, Request]),
   {ok, State}.

build_msg(S = #state{type = batch, batch_size = Size}) ->
   {TsStart, Dist} = batch_start(S),
   Values = batch_points(TsStart, Dist, [], Size),
   flowdata:set_bounds(#data_batch{points = Values})
;
build_msg(#state{type = point, align = false}) ->
   point(faxe_time:now());
build_msg(#state{type = point, align = Unit}) ->
   lager:info(" ~p build point~n",[?MODULE]),
   point(faxe_time:align(faxe_time:now(), Unit)).

batch_start(#state{type = batch, batch_size = Size, every = Every, align = false}) ->
   lager:info("Align is undefined"),
   Ts0 = faxe_time:now() - ((Size + 1) * Every),
   {Ts0, Every};
batch_start(#state{type = batch, batch_size = Size, align = Unit}) ->
   Ts1 = faxe_time:now() - ((Size + 1) * faxe_time:unit_to_ms(Unit)),
   {faxe_time:align(Ts1, Unit), faxe_time:unit_to_ms(Unit)}.

batch_points(_Ts, _Dist, Vals, 0) ->
   Vals;
batch_points(Ts, Dist, Vals, Num) ->
   batch_points(Ts+Dist, Dist, [point(Ts)|Vals], Num - 1).

point(Ts) ->
   #data_point{ts = Ts, fields = [{<<"val">>, random:uniform()*10}]}.