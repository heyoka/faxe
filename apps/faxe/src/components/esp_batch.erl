%% Date: 16.02.2020
%% Ⓒ 2020 heyoka
%% @doc
%% This node is used to batch a number(size) of points. As soon as the node has collected size points it will emit them
%% in a data_batch.
%% A timeout can be set, after which all points currently in the batch
%% will be emitted, regardless of the number of collected points.
%% The timeout is started on the first datapoint coming in to an empty batch.
%%
%%
%%
-module(esp_batch).
-author("Alexander Minichmair").

-behaviour(df_component).

-include("faxe.hrl").

%% API
-export([init/3, process/3, handle_info/2, options/0, wants/0, emits/0, shutdown/1]).

-record(state, {
   size,
   timeout,
   window,
   length = 0,
   timer_ref
}).

options() ->
   [
      {size, integer},
      {timeout, duration, <<"1h">>}
   ].

wants() -> point.
emits() -> batch.

init(_NodeId, _Inputs, #{size := Size, timeout := Timeout0}) ->
   Timeout = case Timeout0 of T when is_binary(T) -> faxe_time:duration_to_ms(T); _Else -> undefined end,
   State = #state{size = Size, timeout = Timeout, window = queue:new()},
   {ok, all, State}.

process(_, #data_point{} = Point, State=#state{} ) ->
   NewState = accumulate(Point, State),
   maybe_emit(NewState);
process(_, #data_batch{points = _Points}, _State=#state{} ) ->
%%   AccFun =
%%   fun(P=#data_point{}, {State, ResList})
%%   Results =

%%   NewState = accumulate(Point, State),
%%   maybe_emit(NewState).
   erlang:error("process databatch not yet implemented in batch node!").

%% this should not be possible, cause the timer starts on an incoming point
handle_info(batch_timeout, State=#state{length = 0}) ->
%%   lager:warning("timeout when Q is empty!!"),
   {ok, State#state{timer_ref = undefined}};
handle_info(batch_timeout, State) ->
   {Batch, NewState} = prepare_batch(State),
   {emit, {1, Batch}, NewState};
handle_info(_Request, State) ->
   {ok, State}.

shutdown(#state{length = 0}) ->
   ok;
shutdown(State) ->
   {Batch, _NewState} = prepare_batch(State),
   dataflow:emit(Batch).



%%%===================================================================
%%% Internal functions
%%%===================================================================
process_batch(Points, State=#state{}) -> ok.

accumulate(Point = #data_point{}, State = #state{window = Win, length = 0}) ->
   NewState = maybe_start_timer(State),
   NewState#state{length = 1, window = queue:in(Point, Win)};
accumulate(Point = #data_point{}, State = #state{window = Win, length = Len}) ->
   State#state{length = Len+1, window = queue:in(Point, Win)}.

maybe_emit(State = #state{size = Length, length = Length}) ->
   {Batch, NewState} = prepare_batch(State),
   {emit, Batch, NewState};
maybe_emit(State = #state{}) ->
   {ok, State}.

prepare_batch(State=#state{window = Win}) ->
   NewState = cancel_timer(State),
   Batch = #data_batch{points = queue:to_list(Win)},
   {Batch, NewState#state{window = queue:new(), length = 0}}.

maybe_start_timer(State = #state{timeout = undefined}) ->
   State;
maybe_start_timer(State = #state{timeout = Timeout}) ->
   TRef = erlang:send_after(Timeout, self(), batch_timeout),
   State#state{timer_ref = TRef}.

cancel_timer(State = #state{timer_ref = undefined}) ->
   State;
cancel_timer(State = #state{timer_ref = TRef}) when is_reference(TRef) ->
   _TLeft = erlang:cancel_timer(TRef),
   State#state{timer_ref = undefined}.


