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
-export([init/3, process/3, handle_info/2, options/0, wants/0, emits/0, shutdown/1, init/4]).

-record(state, {
   size :: non_neg_integer(), %% batch size,
   timeout,
   window :: queue:queue(), %% buffer
   length = 0 :: non_neg_integer(), %% buffer length
   timer_ref
}).

options() ->
   [
      #{name => size, type => integer, desc => <<"Batch-size, number of data-points.">>},
      #{name => timeout, type => duration, default => <<"1h">>,
         desc => <<"Timeout gets started with the first data-point coming in,
            after which the collected data is outputted, regardless of size.">>}
   ].

wants() -> both.
emits() -> batch.

init(_NodeId, _Inputs, #{size := Size, timeout := Timeout0}) ->
   Timeout = case Timeout0 of T when is_binary(T) -> faxe_time:duration_to_ms(T); _Else -> undefined end,
   State = #state{size = Size, timeout = Timeout, window = queue:new()},
   {ok, true, State}.
init(_NodeId, _Inputs, #{size := _Size, timeout := _Timeout0}, #node_state{state = State, ts = Ts}) ->
   %% if there are messages in the buffer, we start a timer, depending on how old the state is already,
   %% if the state is older than the timeout and we have a non empty buffer, we will emit immediately
   %% otherwise we start a timer, that sends a timeout message in TimeoutInterval - StateAge
   NewState =
   case State#state.length == 0 of
      true ->
         %% buffer is empty, nothing to do
         lager:info("the buffer is empty, continue"),
         State#state{timer_ref = undefined};
      false ->
         StateAge = faxe_time:now() - Ts,
         lager:notice("state is ~p ms old and has ~p entries",[StateAge, State#state.length]),
         case StateAge > State#state.timeout of
            true ->
               lager:info("emit buffer"),
               {Batch, NState} = prepare_batch(State),
               dataflow:emit(Batch),
               NState;
            false ->
               lager:info("start timer with timeout ~p",[State#state.timeout - StateAge]),
               TRef = erlang:send_after(State#state.timeout - StateAge, self(), batch_timeout),
               State#state{timer_ref = TRef}
         end
   end,
   {ok, true, NewState}.

process(_, #data_point{} = Point, State=#state{} ) ->
   NewState = accumulate(Point, State),
   maybe_emit(NewState);
process(_, #data_batch{points = Points}, State=#state{} ) ->
   batch_accum(Points, State).

batch_accum(PointList, State = #state{size = Size}) ->
   ResState = lists:foldr(
      fun(Point, CState) ->
         NewState = accumulate(Point, CState),
         %% maybe_emit
         case NewState of
            #state{size = Size, length = Size} ->
               {Batch, NewState1} = prepare_batch(NewState),
               dataflow:emit(Batch),
               NewState1;
            _ -> NewState
         end
      end, State, PointList),
   {ok, ResState}.

%% this should not be possible, cause the timer starts on an incoming point
handle_info(batch_timeout, State=#state{length = 0}) ->
%%   lager:warning("timeout when Q is empty!!"),
%%   erlang:error("batch timeout with no data in batch node!");
   {ok, State};
handle_info(batch_timeout, State) ->
   {Batch, NewState} = prepare_batch(State),
   {emit, {1, Batch}, NewState};
handle_info(_Request, State) ->
   {ok, State}.

shutdown(#state{length = _}) ->
   ok.
%% following clause has been removed, because it would interfere with the state persistence
%%;
%%shutdown(State) ->
%%   {Batch, _NewState} = prepare_batch(State),
%%   dataflow:emit(Batch).



%%%===================================================================
%%% Internal functions
%%%===================================================================

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

prepare_batch(State=#state{window = Win, length = _Len}) ->
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


