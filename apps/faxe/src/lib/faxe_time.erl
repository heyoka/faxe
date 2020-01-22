%% Date: 15.01.17 - 20:37
%% Ⓒ 2017 heykoa
%% @doc
%% faxe's internal timestamp library
%%
%% ALL TIMESTAMPS in faxe are millisecond precision unix_timestamps (erlang:system_time(milli_seconds)) !!
%%
%% this module makes use of erlang's new time functions (since erl 18) and the fantastic 'qdate' library
%% since qdate does not operate with millisecond precision, we have to use a few small tricks before and after
%% using qdate functions
%%
%% Note: this module does not check, if the given timestamps are in ms precision, it is assumed
%%
%% @end
-module(faxe_time).
-author("Alexander Minichmair").


-include("faxe.hrl").
%% type 'timestamp' will stand for sec and ms precision unix timestamp
-export_type([timestamp/0, unit/0, interval/0, duration/0, date/0]).

-type timestamp() :: non_neg_integer().
-type unit()      :: second|minute|hour|day|week.
-type interval()  :: integer().
-type duration()  :: {unit(), interval()}.
-type date()      ::
   {
      {Y::non_neg_integer(), M::non_neg_integer(), D::non_neg_integer()},
      {H::non_neg_integer(), Min::non_neg_integer(), S::non_neg_integer(), Ms::non_neg_integer()}
   }.

%% time units
-define(S, second).
-define(M, minute).
-define(H, hour).
-define(D, day).
-define(W, week).

%% API
-export([
   split_ms/1, add_unit/3, is_on/2,
   beginning_unit/2, get/2, mod/2,
   align/3, to_date/1,
   unit_to_ms/1, now/0, align/2,
   now_date/0,
   binary_to_duration/1,
   duration_to_ms/1,
   add/2,
   to_ms/1,
   now_aligned/1,
   now_aligned/2,
   to_htime/1, send_at/2, to_iso8601/1]).

%% Timer API
-export([
   timer_new/2,
   timer_start/2,
   timer_next/1,
   timer_cancel/1
   , timer_now/1, init_timer/3]).

%%% @doc
%%% get "now" in milliseconds,
%%% very different from erlang:now()
%%% @end
%%
-spec now() -> timestamp().
now() ->
   erlang:system_time(millisecond).


%% @doc
%% convenience function that sends a message to self() with content 'Message'
%% at the desired clock time Timestamp
%%
-spec send_at(timestamp(), term()) -> reference().
send_at(Timestamp, Message) ->
   Time0 = Timestamp - faxe_time:now(),
   Time = case Time0 < 0 of true -> 0; _ -> Time0 end,
   erlang:send_after(Time, self(), Message).

%%% @doc
%%% get "now" date-tuple with milliseconds added
%%% @end
%%
-spec now_date() -> tuple().
now_date() ->
   to_date(faxe_time:now()).

%%% @doc
%%% get the duration of a unit in milliseconds
%%% @end
-spec unit_to_ms(unit()) -> non_neg_integer().
unit_to_ms(millisecond) -> 1;
unit_to_ms(second)      -> 1000;
unit_to_ms(minute)      -> 60000;
unit_to_ms(hour)        -> 3600000;
unit_to_ms(day)         -> 86400000;
unit_to_ms(week)        -> 604800000.


%%% @doc
%%% covert a binary duration expression into a duration-tuple
%%% @end
%%%
-spec binary_to_duration(binary()) -> duration().
binary_to_duration(UnitBin) when is_binary(UnitBin) ->
   {Int, R} = string:to_integer(binary_to_list(UnitBin)),
   {to_unit(R), Int}.

%%% @doc
%%% covert a binary duration expression or a duration-tuple
%%% into a duration presented as milliseconds
%%% @end
%%%
-spec duration_to_ms(duration()|binary()) -> integer().
duration_to_ms(UnitBin) when is_binary(UnitBin) ->
   duration_to_ms(binary_to_duration(UnitBin));
duration_to_ms({TUnit, Int}) ->
   unit_to_ms(TUnit) * Int.

%% @doc
%% add a duration to given ms-timestamp,
%%  ie: 6h
%%
%% like qdate:add_unit but for ms timestamps only
%% @end
%%
-spec add(timestamp(), binary()|integer()) -> timestamp().
add(Ts, DurationBin) when is_binary(DurationBin) ->
   {Unit, Interval} = binary_to_duration(DurationBin),
   add_unit(Ts, Unit, Interval);
add(Ts, DurationMs) when is_integer(DurationMs) ->
   Ts + DurationMs.

%% @doc
%% like qdate:add_unit but for ms timestamps only
%% @end
add_unit(Ts, Unit, Num) ->
   {TsSec, Milli} = split_ms(Ts),
   NewTsSec = qdate:add_unit(Unit, Num, TsSec),
   NewTsSec*1000 + Milli.

%%
%% @doc
%% split a millisecond timestamp in two parts:
%% first element in the tuple is the timestamp reduced to seconds
%% second element is the remaining millisecond portion
%% @end
-spec split_ms(timestamp()) -> {timestamp(), non_neg_integer()}.
split_ms(Ts) when is_integer(Ts) ->
   {erlang:convert_time_unit(Ts, milli_seconds, seconds), ms(Ts)}.

%% @doc
%% find the beginning of a time- unit and get a ms-timestamp
%% @end
%% loosing millisecond precision, no problem, since 1 second is the highest resoltion here
%%
-spec beginning_unit(Unit :: second|minute|hour|day|week|month|year, Ts :: timestamp()) -> timestamp().
beginning_unit(second, Ts) ->
   erlang:convert_time_unit(Ts, milli_seconds, seconds)*1000;
beginning_unit(Unit, Ts) ->
   NewTsSec = erlang:convert_time_unit(Ts, milli_seconds, seconds),
   QFun = list_to_existing_atom("beginning_" ++ atom_to_list(Unit)),
   D = qdate:QFun(NewTsSec),
   TsNew = qdate:to_unixtime(D)*1000,
%%   lager:info("~nTs b : ~p | ~p |~nTs a : ~p | ~p |",[Ts, qdate:to_date(NewTsSec), TsNew , D]),
%%   lager:info("Ts a : ~p",[qdate:to_unixtime(D)*1000]),
   TsNew.

%% @doc get now(), but aligned to a unit-interval
%% @see align()
-spec now_aligned(duration()) -> timestamp().
now_aligned({Unit, Interval}) ->
   align(faxe_time:now(), Unit, Interval).

now_aligned(Unit, Interval) ->
   now_aligned({Unit, Interval}).

%% @doc
%% get a timestamp, where the unit is set to the nearest past interval occurrence
%% @end
-spec align(timestamp(), duration()) -> timestamp().
align(Ts, {Unit, Interval} = _Granularity) ->
   align(Ts, Unit, Interval).

-spec align(timestamp(), unit(), interval()) -> timestamp().
align(Ts, Unit, Interval) ->
   Intval = interval(Interval),
   DateTime = datetime_parts(Ts),
   UnitValue = get(Unit, DateTime),
%%   lager:info("UnitValue:: ~p",[UnitValue]),
   Distance = mod(UnitValue, Intval),
%%   lager:info("Distance:: ~p",[Distance]),
   NewUnitValue = UnitValue - Distance,
   D = set_to_unit(Unit, DateTime, NewUnitValue),
%%   lager:info("Date : ~p",[D]),
   qdate:to_unixtime(D)*1000.

%%% @doc
%%% convert a ms-timestamp into a date-tuple with ms
%%% @end
-spec to_date(timestamp()) -> tuple().
to_date(Ts) ->
   datetime_parts(Ts).

%%% @doc
%%% convert a ms-timestamp into a time-tuple with ms
%%% @end
-spec to_htime(timestamp()) -> tuple().
to_htime(Ts) ->
   {_Date, T} = to_date(Ts),
   T.

%% @doc convert a timestamp or a datetime tuple into a iso8601 binary string
%% returns an empty binary if anything other is given as input
-spec to_iso8601(timestamp()|tuple()) -> binary().
to_iso8601(Ts) when is_integer(Ts) ->
   to_iso8601(to_date(Ts));
to_iso8601({{_Year, _Month, _Day}=D,{H, Min, SecFrac}}) when is_float(SecFrac) ->
   {Sec, Milli} = split_frac_sec(SecFrac),
   to_iso8601({D, {H, Min, Sec, Milli}});
to_iso8601({{Year, Month, Day},{H, Min, S, Milli}}) ->
   Fmt = "~.4.0w-~.2.0w-~.2.0wT~.2.0w:~.2.0w:~.2.0w.~.3.0wZ",
   iolist_to_binary(io_lib:format(Fmt, [Year,Month,Day,H,Min,S, Milli]));
to_iso8601(_) ->
   <<>>.

to_ms({Date, {H, Min, SecFrac}}) when is_float(SecFrac) ->
   {Sec, Milli} = split_frac_sec(SecFrac),
   to_ms({Date, {H, Min, Sec, Milli}});
to_ms({Date,{H, Min, S, Milli}}) ->
   qdate:to_unixtime({Date,{H, Min, S}}) * 1000 + Milli;
to_ms(Date) when is_tuple(Date) ->
   qdate:to_unixtime(Date)*1000.

%% 41.223 -> sec.milli
-spec split_frac_sec(float()) -> tuple().
split_frac_sec(SecFrac) when is_float(SecFrac)->
   Sec = erlang:trunc(SecFrac),
   {Sec, faxe_util:decimal_part(SecFrac, 3)}.

%%% @doc
%%% get the unit portion from a ms-timestamp
%%% @end
-spec get(millisecond|second|minute|hour|day|week|month|year, timestamp()) -> non_neg_integer().
get(Unit, Ts) when is_integer(Ts) ->
   get(Unit, datetime_parts(Ts));
get(millisecond, Ts) ->
   {_Date,{_H,_Min,_Second,Milli}} = Ts,
   Milli;
get(second, Ts) ->
   {_Date,{_H,_Min,Second,_M}} = Ts,
   Second;
get(minute, Ts) ->
   {_Date,{_H,Minute,_S,_M}} = Ts,
   Minute;
get(hour, Ts) ->
   {_Date,{H,_Min,_S,_M}} = Ts,
   H;
get(day_of_week, Ts) ->
   {Date,_T} = Ts,
   calendar:day_of_the_week(Date);
get(day_of_week_string, Ts) ->
   httpd_util:day(get(day_of_week, Ts));
get(day, Ts) ->
   {{_Y, _M, Day},_T} = Ts,
   Day;
get(week, Ts) ->
   {Date, _T} = Ts,
   {_Year, Week} = calendar:iso_week_number(Date),
   Week;
get(month, Ts) ->
   {{_Y, Month, _D},_T} = Ts,
   Month;
get(month_string, Ts) ->
   httpd_util:month(get(month, Ts));
get(year, Ts) ->
   {{Year, _M, _D},_T} = Ts,
   Year.



%% check if millisecond timestamp is exactly on the beginning of the given unit
-spec is_on(second|minute|hour|day, timestamp()) -> true|false.
is_on(second, Ts) ->
   case datetime_parts(Ts) of
      {_Date,{_Hour,_Minute,_Second,0}} -> true;
      _O -> false
   end;
is_on(minute, Ts) ->
   case datetime_parts(Ts) of
      {_Date,{_Hour,_Minute,0,0}} -> true;
      _O -> false
   end;
is_on(hour, Ts) ->
   case datetime_parts(Ts) of
      {_Date,{_Hour,0,0,0}} -> true;
      _O -> false
   end;
is_on(day, Ts) ->
   case datetime_parts(Ts) of
      {_Date,{0,0,0,0}} -> true;
      _O -> false
   end.


%%%%%%%%%%%%%% Internal %%%%%%%%%%%%%%%%%%%%%%

to_unit("ms") -> millisecond;
to_unit("s") -> second;
to_unit("m") -> minute;
to_unit("h") -> hour;
to_unit("d") -> day;
to_unit("w") -> week;
to_unit("y") -> year.

%% note: returns now() format without milliseconds
%% is also a 'rounding' timestamp function
set_to_unit(second, {Date, {Hour, Min, _Sec, _M}}, NewSec) ->
   {Date, {Hour, Min, NewSec}};
set_to_unit(minute, {Date, {Hour, _Min, _Sec, _M}}, NewMin) ->
   {Date, {Hour, NewMin, 0}};
set_to_unit(hour, {Date, {_Hour, _Min, _Sec, _M}}, NewHour) ->
   {Date, {NewHour, 0, 0}};
set_to_unit(day, {{_Year, _Month, _Day}, _Time}, NewDay) ->
   {{_Year, _Month, NewDay}, {0,0,0}};
set_to_unit(month, {{_Year, _Month, _Day}, _Time}, NewMonth) ->
   {{_Year, NewMonth, 1}, {0,0,0}};
set_to_unit(year, {{_Year, _Month, _Day}, _Time}, NewYear) ->
   {{NewYear, 1, 1}, {0,0,0}}.



-spec datetime_parts(timestamp()) -> tuple().
datetime_parts(Ts) when is_integer(Ts) ->
   MicroTs = Ts * 1000,
   TS = {_, _, Micro1} = { MicroTs div 1000000000000,
      MicroTs div 1000000 rem 1000000,
      MicroTs rem 1000000},

   {{Year,Month,Day},{Hour,Minute,Second}} = calendar:now_to_universal_time(TS),
   Milli = Micro1 div 1000 rem 1000,
   {{Year,Month,Day},{Hour,Minute,Second,Milli}}.


%% get the millisecond portion of the given timestamp
-spec ms(timestamp()) -> non_neg_integer().
ms(Ts) ->
   MicroTs = Ts * 1000,
   Micro1 = MicroTs rem 1000000,
   Micro1 div 1000 rem 1000.

mod(X,Y) -> r_mod(X, Y).
r_mod(X,Y) when X > 0 -> X rem Y;
r_mod(X,Y) when X < 0 -> Y + X rem Y;
r_mod(0,_Y) -> 0.

interval(V) when is_integer(V) andalso V /= 0 -> V;
interval(_) -> 1.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% timer %%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% @doc
%% init and start a timer, possibly aligned to a multiple of Interval
%% note that Interval is a duration() / binary here, ie: 15s
%% if Align is given as false, this function has the same behaviour as the "timer_start" function
%%
-spec init_timer(true|false, duration(), term()) -> #faxe_timer{}.
init_timer(Align, Interval, Message) when is_atom(Align), is_binary(Interval) ->
   Now = faxe_time:now(),
   NewTs =
      case Align of
         true -> faxe_time:align(Now, faxe_time:binary_to_duration(Interval));
         false -> Now
      end,
   TRef = faxe_time:send_at(NewTs, Message),
   Timer = #faxe_timer{interval = faxe_time:duration_to_ms(Interval),
      message = Message, last_time = NewTs, timer_ref = TRef},
   Timer.

%% @doc create a new timer instance
-spec timer_new(non_neg_integer(), term()) -> #faxe_timer{}.
timer_new(Interval, Message) ->
   #faxe_timer{
      interval = Interval,
      message = Message
   }.

%% @doc create a timer and immediately send the timeout message without waiting for the first interval to elapse
-spec timer_start(non_neg_integer(), term()) -> #faxe_timer{}.
timer_start(Interval, Message) ->
   T = timer_new(Interval, Message),
   timer_now(T).

%% @doc sends the timeout message with timeout 0
-spec timer_now(#faxe_timer{}) -> #faxe_timer{}.
timer_now(Timer = #faxe_timer{message = Message}) ->
   Now = faxe_time:now(),
%%   lager:debug("timer start for: ~p" ,[Now]),
   TRef = erlang:send_after(0, self(), Message),
   Timer#faxe_timer{last_time = Now, timer_ref = TRef}.

%% @doc sends the timeout message in interval milliseconds
-spec timer_next(#faxe_timer{}) -> #faxe_timer{}.
timer_next(Timer = #faxe_timer{interval = Interval, message = Message, last_time = Last}) ->
   NewAt = Last + Interval,
%%   lager:notice("timer next send in: ~p ms for :~p || ~p",[NewAt-faxe_time:now(), NewAt, Timer]),
   TRef = faxe_time:send_at(NewAt, Message),
   Timer#faxe_timer{last_time = NewAt, timer_ref = TRef}.

%% @doc cancel a running timer
-spec timer_cancel(#faxe_timer{}) -> #faxe_timer{}.
timer_cancel(Timer = #faxe_timer{timer_ref = undefined}) ->
   Timer;
timer_cancel(Timer = #faxe_timer{timer_ref = TRef}) ->
%%   lager:notice("timer cancel: ~p", [Timer]),
   catch erlang:cancel_timer(TRef),
   Timer#faxe_timer{timer_ref = undefined}.


