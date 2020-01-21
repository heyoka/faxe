%%%-----------------------------------------------------------------------------
%%% @doc
%%% backoff module, for reconnecting and other retry things
%%%
%%% @end
%%%-----------------------------------------------------------------------------
-module(faxe_backoff).

-author('miae@tgw-group.com').

-export([new/0, new/1, execute/2, reset/1,
   set_min_interval/2, set_max_interval/2, set_max_retries/2]).


-define(MIN_INTERVAL, 300).
-define(MAX_INTERVAL, 5000).

-define(IS_MAX_RETRIES(Max), (is_integer(Max) orelse Max =:= infinity)).

-record(backoff, {
   min_interval  = ?MIN_INTERVAL,
   max_interval  = ?MAX_INTERVAL,
   max_retries   = infinity,
   interval      = ?MIN_INTERVAL,
   retries       = 0,
   timer         = undefined}).

-opaque backoff() :: #backoff{}.

-export_type([backoff/0]).

%%------------------------------------------------------------------------------
%% @doc Create a backoff.
%% @end
%%------------------------------------------------------------------------------
-spec new() -> backoff().
new() ->
   new({?MIN_INTERVAL, ?MAX_INTERVAL}).

%%------------------------------------------------------------------------------
%% @doc Create a backoff with min_interval, max_interval seconds and max retries.
%% @end
%%------------------------------------------------------------------------------
-spec new(MinInterval) -> backoff() when
   MinInterval  :: non_neg_integer() | {non_neg_integer(), non_neg_integer()}.
new(MinInterval) when is_integer(MinInterval), MinInterval =< ?MAX_INTERVAL ->
   new({MinInterval, ?MAX_INTERVAL});

new({MinInterval, MaxInterval})
      when is_integer(MinInterval), is_integer(MaxInterval), MinInterval =< MaxInterval ->
   new({MinInterval, MaxInterval, infinity});
new({_MinInterval, _MaxInterval}) ->
   new({?MIN_INTERVAL, ?MAX_INTERVAL, infinity});
new({MinInterval, MaxInterval, MaxRetries}) when is_integer(MinInterval),
      is_integer(MaxInterval), ?IS_MAX_RETRIES(MaxRetries) ->
   #backoff{min_interval = MinInterval,
      interval     = MinInterval,
      max_interval = MaxInterval,
      max_retries  = MaxRetries}.

-spec set_min_interval(#backoff{}, non_neg_integer()) -> #backoff{}.
set_min_interval(R = #backoff{}, NewMinInterval) ->
   R#backoff{min_interval = NewMinInterval}.

-spec set_max_interval(#backoff{}, non_neg_integer()) -> #backoff{}.
set_max_interval(R = #backoff{}, NewMaxInterval) ->
   R#backoff{max_interval = NewMaxInterval}.

-spec set_max_retries(#backoff{}, non_neg_integer()) -> #backoff{}.
set_max_retries(R = #backoff{}, NewMaxRetries) ->
   R#backoff{max_retries = NewMaxRetries}.

%%------------------------------------------------------------------------------
%% @doc Execute backoff.
%% @end
%%------------------------------------------------------------------------------
-spec execute(Backoff, TimeoutMsg) -> {stop, retries_exhausted} | {ok, backoff()} when
   Backoff :: backoff(),
   TimeoutMsg :: tuple().
execute(#backoff{retries = Retries, max_retries = MaxRetries}, _TimeoutMsg) when
   MaxRetries =/= infinity andalso (Retries > MaxRetries) ->
   {stop, retries_exhausted};

execute(Backoff = #backoff{
   interval     = Interval,
   retries      = Retries,
   timer        = Timer}, TimeoutMsg) ->
   % cancel timer first...
   cancel(Timer),
%%   lager:notice("backoff interval: ~p", [Interval]),
   NewTimer = erlang:send_after(Interval, self(), TimeoutMsg),
   {ok,
      Backoff#backoff{interval = next_interval(Backoff), retries = Retries+1, timer = NewTimer }}.

next_interval(#backoff{min_interval = MinInt, max_interval = MaxInt, interval = Int}) ->
   Interval0 = Int * 2,
   NewInterval =
      if
         Interval0 > MaxInt -> MinInt;
         true -> Interval0
      end,
   NewInterval.

%%------------------------------------------------------------------------------
%% @doc Reset backoff
%% @end
%%------------------------------------------------------------------------------
-spec reset(backoff()) -> backoff().
reset(B = #backoff{min_interval = MinInterval, timer = Timer}) ->
   cancel(Timer),
   B#backoff{interval = MinInterval, retries = 0, timer = undefined}.

cancel(undefined) -> ok;
cancel(Timer) when is_reference(Timer) -> erlang:cancel_timer(Timer).

