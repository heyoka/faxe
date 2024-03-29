%%%-------------------------------------------------------------------
%%% @author heyoka
%%% @copyright (C) 2022, <COMPANY>
%%% @doc
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(s7reader).

-behaviour(gen_server).

-export([start_link/1, register/3, get_stats/1, add_read_stats/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
  code_change/3]).
-export([decode_dt/1]).

-define(SERVER, ?MODULE).
-define(EMPTY_RETRY_INTERVAL, 300).

-define(STAT_LIST_LENGTH, 50).

-include("faxe.hrl").

-record(state, {
  s7_ip                           :: binary(),
  port              = 102         :: non_neg_integer(),
  conn_opts         = #{}         :: map(),
  %% all addresses by read interval
  current_addresses = #{}         :: map(),
  %% timers for every client read interval
  slot_timers       = []          :: list(),
  %% busy flag
  busy              = false       :: true|false,
  connected         = false       :: true|false,
  %% protocol PDU size, will be fetched from the slave, once connected
  pdu_size          = 240         :: non_neg_integer(),
  %% wether to try to get the PDU size on connect
  retrieve_pdu_size = true        :: true|false,
  %% ready built (optimized) addresses / cache
  request_cache     = #{}         :: map(),
  %% pdu-size of cached addresses
  cache_pdu_size    = 240         :: non_neg_integer(),
  %% read timer
  timer             = undefined   :: undefined|reference()
}).

%%%===================================================================
%%% Spawning and gen_server implementation
%%%===================================================================
register(Opts = #{ip := Ip}, Interval, Vars) when is_list(Vars) ->
  %% make sure a reader is running
  %% ask supervisor to get child
  Children = supervisor:which_children(s7reader_sup),
  ChildList =
  [Pid || {Name, Pid, _Type, _Modules} <- Children, Name =:= Ip],
  Server =
  case ChildList of
    [C] when is_pid(C) -> C;
    [undefined] ->
      %% the child pid is undefined, when it has stopped previously, so we simply restart it
      case supervisor:restart_child(s7reader_sup, Ip) of
        {ok, Child} -> Child;
        {error, not_found} -> start_reader(Opts);
        {error, _What} ->
          catch supervisor:delete_child(s7reader_sup, Ip),
          start_reader(Opts)
      end;
    _ -> start_reader(Opts)
end,
%%  lager:notice("So ~p wants to read: ~p every ~pms from ~p",[self(), Vars, Interval, Ip]),
  Server ! {register, Interval, Vars, self()}.

start_reader(Opts = #{ip := _Ip}) ->
  %% we possibly get {error, {already_started, pid()}} here
  %% maybe with very fast succession of requests to register() ->
  %% that can happen, if we have multiple s7read nodes in one flow reading form the same plc
  case s7reader_sup:start_reader(Opts) of
    {ok, ServerPid} -> ServerPid;
    {error, {already_started, SPid}} -> SPid
  end.

add_read_stats(Ip, Stats) ->
  NewStats =
  case ets:lookup(s7reader_stats, Ip) of
    [] -> [Stats];
    [{Ip, Stats0}] ->
      Stats1 = [Stats|Stats0],
      lists:sublist(Stats1, ?STAT_LIST_LENGTH)
  end,
  ets:insert(s7reader_stats, {Ip, NewStats})
  .

add_tag_stats(Ip, Stats) ->
%%  NewStats =
%%    case ets:lookup(s7reader_tag_stats, Ip) of
%%      [] -> [Stats];
%%      [{Ip, Stats0}] ->
%%        Stats1 = [Stats|Stats0],
%%        lists:sublist(Stats1, ?STAT_LIST_LENGTH)
%%    end,
%%  ets:insert(s7reader_tag_stats, {Ip, NewStats})
  ets:insert(s7reader_tag_stats, {Ip, Stats})
.


get_stats(Ip) ->
  NumClients = case ets:lookup(s7reader_clients, Ip) of
              [] -> 0;
              [{Ip, Clients}] -> length(Clients)
            end,
  OutStats =
  case ets:lookup(s7reader_stats, Ip) of
    [] ->
      #{<<"avg_time">> => 0, <<"avg_num_req">> => 0, <<"avg_num_conn">> => 0};
    [{Ip, Stats}] when is_list(Stats) ->
      Length = length(Stats),
      StatsFun =
        fun(#{time := T, num_req := NumReq, num_conn := NumConn}, {Ts, Rs, Cs})
          -> {[T|Ts], [NumReq|Rs], [NumConn|Cs]}
        end,

      {Times, Reqs, Conns} = lists:foldl(StatsFun, {[], [], []}, Stats),
      #{<<"read_time_ms">> =>
        #{<<"avg">> => round(lists:sum(Times)/Length),
          <<"min">> => lists:min(Times),
          <<"max">> => lists:max(Times)},
        <<"read_num_req">> => #{<<"avg">> => round(lists:sum(Reqs)/Length)},
        <<"read_num_conn">> => #{<<"avg">> => round(lists:sum(Conns)/Length)},
        <<"num_client_nodes">> => NumClients
      }
  end,
  maps:merge(OutStats, get_tag_stats(Ip)).

get_tag_stats(Ip) ->
  case ets:lookup(s7reader_tag_stats, Ip) of
    [] ->
      #{<<"avg_addresses">> => 0, <<"avg_read_addresses">> => 0};
    [{Ip, #{num_tags := NumTags, num_read_tags := NumRTags}}] ->
      #{<<"avg_addresses">> => NumTags, <<"avg_read_addresses">> => NumRTags}

%%    [{Ip, Stats}] when is_list(Stats) ->
%%      Length = length(Stats),
%%      StatsFun =
%%        fun(#{num_tags := NumTags, num_read_tags := NumRTags}, {Tags, ReadTags}) ->
%%          {[NumTags|Tags], [NumRTags|ReadTags]}
%%        end,
%%      {AllNumTags, AllReadNumTags} = lists:foldl(StatsFun, {[],[]}, Stats),
%%      #{<<"avg_addresses">> => round(lists:sum(AllNumTags)/Length),
%%        <<"avg_read_addresses">> => round(lists:sum(AllReadNumTags)/Length)}
  end.


start_link(Opts) ->
  gen_server:start_link(?MODULE, Opts, []).

init(Opts0=#{ip := Ip}) ->
  %% monitor time_offsets
  erlang:monitor(time_offset, clock_service),
  Opts = maps:without([vars], Opts0),
  %% lets see, if we have clients in ets clients table
  State = init_clients(Ip, Opts),
  %% connect pool manager
  s7pool_manager:connect(Opts),
  {ok, State}.

init_clients(Ip, Opts) ->
  Clients = get_clients(Ip),
  lists:foldl(
    fun({ClientPid, Interval, Vars}, StateAcc) ->
      case is_process_alive(ClientPid) of
        true ->
          do_register(ClientPid, Interval, Vars, StateAcc);
        false ->
          %% delete from ets
          remove_client_ets(ClientPid, Ip),
          StateAcc
      end
    end,
    #state{s7_ip = Ip, conn_opts = Opts},
    Clients).

handle_call(_Request, _From, State = #state{}) ->
  {reply, ok, State}.

handle_cast(_Request, State = #state{}) ->
  {noreply, State}.


%% command for reading the next timeslot, for the result, we just use the timestamp of the first
%% timer in the timer-list, they all are the same anyway
handle_info({read, Requests, [{_Intv, #faxe_timer{last_time = Ts}}|_] = SendTimers},
    State=#state{slot_timers = SlotTimers, conn_opts = Opts, s7_ip = Ip}) ->

  %% read
  case read(Requests, Opts, Ip) of
    {true, ReadResults} -> emit_results(ReadResults, Ts);
    _ -> ok
  end,

  %% progress timers used
  NewSlotTimers =
    lists:foldl(
      fun({IntervalKey, STimer = #faxe_timer{last_time = LastTs, interval = Interval}}, Timers) ->
        T = {IntervalKey, STimer#faxe_timer{last_time = LastTs+Interval}},
        lists:keyreplace(IntervalKey, 1, Timers, T)
      end,
      SlotTimers,
      SendTimers),
  NewState = State#state{busy = false, slot_timers = NewSlotTimers},
  maybe_next(NewState);

handle_info(try_read, State) ->
  maybe_next(State);
handle_info({register, Interval, Vars, ClientPid}, State = #state{}) ->
  NewState =
    case add_client_ets(ClientPid, Interval, Vars, State) of
      true -> do_register(ClientPid, Interval, Vars, State);
      false -> State
    end,
  {noreply, NewState};
handle_info({'DOWN', _MonitorRef, process, Client, _Info}, State = #state{}) ->
  NewState = remove_client(Client, State),
  %% stop, if we have no clients left
  case get_clients(State) of
    [] -> {stop, normal, NewState};
    _ -> {noreply, NewState}
  end;
handle_info({s7_connected, _Client}=M, State = #state{connected = true, retrieve_pdu_size = false}) ->
  %% alread connected / idempotency
  send_all_clients(M, State),
  maybe_next(State);
handle_info({s7_connected, _Client}=M,
    State = #state{s7_ip = Ip, cache_pdu_size = CachePDU, request_cache = Cache, pdu_size = InitialPDUSize}) ->
  {PduSize, Retrieve} =
    case catch s7pool_manager:get_pdu_size(Ip) of
      {ok, PS} -> {PS, false};
      _O ->
        lager:notice("getting pdu size failed: ~p", [_O]),
        {InitialPDUSize, true}
    end,
%%  lager:notice("pdu size is ~p bytes",[PduSize]),
  send_all_clients(M, State),
  %% clear the cache, if pdu size changed
  NewCache =
  case CachePDU == PduSize of
    true -> Cache;
    false -> #{}
  end,
  maybe_next(State#state{pdu_size = PduSize, connected = true, retrieve_pdu_size = Retrieve, request_cache = NewCache});
handle_info({s7_disconnected, _Client}=M, State = #state{timer = Timer}) ->
  catch erlang:cancel_timer(Timer),
  send_all_clients(M, State),
  {noreply, State#state{timer = undefined, busy = false, connected = false}};
handle_info({'CHANGE', _MonitorReference, time_offset, clock_service, _NewTimeOffset}, State=#state{timer = Timer}) ->
  catch erlang:cancel_timer(Timer),
  lager:warning("<~p> TIME_OFFSET changed ", [{?MODULE, self()}]),
  NewState = State#state{slot_timers = reset_slot_timers(State), timer = undefined},
  maybe_next(NewState);
handle_info(_Info, State = #state{}) ->
  maybe_next(State).

terminate(_Reason, _State = #state{s7_ip = Ip}) ->
  %% cleanup stats entries
  catch ets:delete(s7reader_stats, Ip),
  catch ets:delete(s7reader_tag_stats, Ip),
  ok.

code_change(_OldVsn, State = #state{}, _Extra) ->
  {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
do_register(ClientPid, Interval, Vars, State = #state{current_addresses = CurrentSlots, connected = Connected}) ->
  erlang:monitor(process, ClientPid),
  Current = maps:get(Interval, CurrentSlots, []),
  NewCurrent = s7_utils:add_client_addresses(ClientPid, Vars, Current),
  NewSlots = CurrentSlots#{Interval => NewCurrent},
  %% inform client
  Msg = case Connected of true -> s7_connected; false -> s7_disconnected end,
  ClientPid ! {Msg, undefined},
  {noreply, NewState} = maybe_next(State#state{current_addresses = NewSlots, request_cache = #{}}),
  NewState.


send_all_clients(Msg, S=#state{}) ->
  lists:foreach(fun({ClientPid, _, _}) -> ClientPid ! Msg end, get_clients(S)).

maybe_next(State = #state{connected = false}) ->
%%  lager:notice("maybe_next when not connected"),
  {noreply, State};
maybe_next(State = #state{busy = true}) ->
  {noreply, State};
maybe_next(State = #state{current_addresses = Slots}) when map_size(Slots) == 0 ->
  erlang:send_after(?EMPTY_RETRY_INTERVAL, self(), try_read),
  {noreply, State#state{busy = false}};
maybe_next(State = #state{current_addresses = Slots}) ->
  NewSlotTimers0 = check_slot_timers(Slots, State#state.slot_timers),
  [{_I0, #faxe_timer{last_time = At0, interval = Interval}}|_] = ReadIntervals0 = next_read(NewSlotTimers0),
  Now = faxe_time:now(),
  TimeDiff = Now - At0,
  {ReadIntervals, NewSlotTimers, At} =
  case TimeDiff > Interval of
    true ->
      NewSlotTimers1 = reset_slot_timers(State),
      [{_I1, #faxe_timer{last_time = At1}}|_] = ReadIntervals1 = next_read(NewSlotTimers1),
%%      lager:warning("~p TimeDrift of ~p detected, reset timers ... (~p vs ~p)",
%%        [{?MODULE, self(), Now}, TimeDiff, faxe_time:to_iso8601(At0), faxe_time:to_iso8601(At1)]),
      {ReadIntervals1, NewSlotTimers1, At1};
    false ->
      {ReadIntervals0, NewSlotTimers0, At0}
  end,
  Intervals = proplists:get_keys(ReadIntervals),
  {ReadRequests, NewState} = get_requests(Intervals, State),
  NewTimer = faxe_time:send_at(At, {read, ReadRequests, ReadIntervals}),
  {noreply, NewState#state{busy = true, slot_timers = NewSlotTimers, timer = NewTimer}}.


check_slot_timers(Slots, CurrentTimers) ->
  SlotIntervals = maps:keys(Slots),
  lists:foldl(
    fun(SlotInterval, Timers) ->
      case lists:keymember(SlotInterval, 1, Timers) of
        true -> Timers;
        false ->
          NewTimer0=#faxe_timer{last_time = LastTime, interval = Interval} = faxe_time:timer_new(SlotInterval, true, read),
          %% init timers on the NEXT interval
          [{SlotInterval, NewTimer0#faxe_timer{last_time = LastTime+Interval}}|Timers]
      end
    end,
    CurrentTimers,
    SlotIntervals
  ).


%% reset all timer records
-spec reset_slot_timers(#state{}) -> list().
reset_slot_timers(#state{current_addresses = Slots}) ->
  NewTimers = check_slot_timers(Slots, []),
  NewTimers.
%%  State#state{slot_timers = NewTimers}.


next_read(SlotTimers) ->
  [{_I, #faxe_timer{last_time = NextTs}}|_] =
    lists:usort(fun({_IntervalA, #faxe_timer{last_time = A}}, {_IntervalB, #faxe_timer{last_time = B}}) -> A =< B end,
      SlotTimers),
  FunProceed =
    fun({_Interval, #faxe_timer{last_time = LastTime}} = SlotT, ReadIntervals) ->
      case LastTime == NextTs of
        true -> [SlotT|ReadIntervals];
        false -> ReadIntervals
      end
    end,
  NextSlotsToRead = lists:foldl(FunProceed, [], SlotTimers),
  NextSlotsToRead.

get_requests(IntervalList, S = #state{request_cache = Cache}) when is_map_key(IntervalList, Cache) ->
  {maps:get(IntervalList, Cache), S};
get_requests([Interval] = IntervalList, S = #state{current_addresses = Slots}) ->
  Addresses = maps:get(Interval, Slots),
  build_requests(IntervalList, Addresses, S);
get_requests(IntervalList, S = #state{current_addresses = Slots}) ->
  Addresses =
  lists:foldl(
    fun(ReadInterval, Vars) ->
      SlotVars = maps:get(ReadInterval, Slots),
      s7_utils:merge_addresses(SlotVars, Vars)
    end, [], IntervalList),
  build_requests(IntervalList, Addresses, S).

build_requests(IntervalList, Addresses, S=#state{pdu_size = PDUSize, request_cache = Cache}) ->
  {_T, {NumReadReq, BuiltRequests}} = timer:tc(s7_utils, build_addresses, [Addresses, PDUSize]),
%%  lager:notice("BuiltRequests: ~p",[BuiltRequests]),
  add_tag_stats(S#state.s7_ip, #{num_tags => length(Addresses), num_read_tags => NumReadReq}),
  {BuiltRequests, S#state{request_cache = Cache#{IntervalList => BuiltRequests}, cache_pdu_size = PDUSize}}.


add_client_ets(ClientPid, Interval, Vars, #state{s7_ip = Ip}) ->
  case ets:lookup(s7reader_clients, Ip) of
    [] ->
      ets:insert(s7reader_clients, {Ip, [{ClientPid, Interval, Vars}]}),
      true;
    [{Ip, Clients}] ->
      case lists:keymember(ClientPid, 1, Clients) of
        true ->
          false;
        false ->
          ets:insert(s7reader_clients, {Ip, [{ClientPid, Interval, Vars}|Clients]}),
          true
      end
  end.

get_clients(#state{s7_ip = Ip}) ->
  get_clients(Ip);
get_clients(Ip) ->
  case ets:lookup(s7reader_clients, Ip) of
    [] -> [];
    [{Ip, Clients}] -> Clients
  end.

remove_client_ets(ClientPid, #state{s7_ip = Ip}) ->
  remove_client_ets(ClientPid, Ip);
remove_client_ets(ClientPid, Ip) ->
  case ets:lookup(s7reader_clients, Ip) of
    [] -> [];
    [{Ip, Clients}] ->
      NewClients = lists:keydelete(ClientPid, 1, Clients),
      ets:insert(s7reader_clients, {Ip, NewClients})
  end.

remove_client(Client, State = #state{current_addresses = AddressSlots, slot_timers = STimers}) ->
  {NewAddressSlots, SlotTimers} =
  case lists:keytake(Client, 1, get_clients(State)) of
    {value, {Client, Interval, Vars}, _Clients2} ->
      remove_client_ets(Client, State),
      case catch maps:take(Interval, AddressSlots) of
        {Addresses, RemainingSlots} when is_list(Addresses) ->
          %% remove client from clients and addresses
          Removed = s7_utils:remove_client(Client, Vars, Addresses),
%%          lager:info("after removing addresses are: ~p",[Removed]),
          case Removed of
            [] ->
              %% no more clients/vars left for this timeslot, so we delete the slot-timer also
              {RemainingSlots, proplists:delete(Interval, STimers)};
            Res when is_list(Res) -> {RemainingSlots#{Interval => Res}, STimers}
          end;
        _ ->
          {AddressSlots, STimers}
      end;
    false ->
      {AddressSlots, STimers}
  end,
  State#state{current_addresses = NewAddressSlots, slot_timers = SlotTimers, request_cache = #{}}.


read(Requests, Opts, Ip) ->
  RunWith = s7pool_manager:connection_count(Ip),
  case RunWith > 0 of
    true -> do_read(Requests, Opts, RunWith);
    false -> lager:warning("~p no connection when trying to read vars", [?MODULE]), {error, no_connection}
  end.

do_read(Requests, Opts=#{ip := Ip}, RunWith) ->
  ElFun =
    fun
      ({Vars, Aliases}, {true, Results}) ->
        case s7pool_manager:read_vars(Opts, Vars) of
          {ok, Res} ->
            {true, [handle_result(Res, Aliases)|Results]};
          Other ->
            lager:warning("Error reading vars: ~p",[Other]),
            {false, Results}
        end;
      ({_Vars, _Aliases}, {false, _} = R) ->
        R
    end,
  {Time, ReadResult} = timer:tc(plists, fold, [ElFun, {true, []}, Requests, {processes, RunWith}]),
  TimeMillis = erlang:round(Time/1000),
  NumReqs = length(Requests),
  add_read_stats(Ip, #{time => TimeMillis, num_req => NumReqs, num_conn => RunWith}),
  ReadResult.

emit_results([FirstResult|RequestResults], Ts) ->
  MergeFun =
    fun
      (Prev, Val) when is_map(Prev), is_map(Val) -> maps:merge(Prev, Val);
      (Prev, Val) when is_list(Prev), is_list(Val) -> Prev++Val;
      (_, Val) -> Val
    end,
  TheResult =
    case (catch mapz:deep_merge(MergeFun, FirstResult, RequestResults)) of
      R1 when is_map(R1) -> R1;
      Nope ->
        lager:warning("could not read a valid result: ~p",[Nope]),
        #{}
    end,

  maps:map(fun(ClientPid, Values) ->
    ClientPid ! {s7_data, Ts, Values}
           end, TheResult).

%%% handle results  -->
handle_result(ResultList, AliasesList) when is_list(ResultList), is_list(AliasesList) ->
  do_handle_result(ResultList, AliasesList, #{}).

do_handle_result([], [], BuiltResult) ->
  BuiltResult;
do_handle_result([Res|R], [Aliases|AliasesList], Acc) ->
  {Clients, Value} = _ClientValues = bld(Res, Aliases),
%%  lager:info("Value: ~p for clients: ~p",[Value, Clients]),
  do_handle_result(R, AliasesList, client_vars(Value, Clients, Acc)).

%% bools or string
client_vars([], [], ClientVars) ->
  ClientVars;
client_vars([Value|Values], [Clients|CRest], ClientVars) ->
  client_vars(Values, CRest, client_vars(Value, Clients, ClientVars));
client_vars(Value, Clients, ClientVars) ->
  F = fun
        ({ClientPid, FieldName}, Acc) when is_map_key(ClientPid, Acc) ->
          CurrentClientVars = maps:get(ClientPid, Acc),
          Acc#{ClientPid => [{FieldName, Value}|CurrentClientVars]};
        ({ClientPid1, FieldName1}, Acc1) ->
          Acc1#{ClientPid1 => [{FieldName1, Value}]}
      end,
  lists:foldl(F, ClientVars, Clients).


%% string is a special case, multiple chars(bytes) form one string
-spec bld(list()|binary(), {list(), list()}) -> {list(), list()}.
bld(Res, {[As], [string]}) ->
%%  lager:warning("~p bld string single: ~p, ~p",[?MODULE, Res, As]),
  Data = decode(string, Res),
  {[As], [Data]};
%% non-bool, non-single, string
bld(Res, {As, [DType|_]}) ->
%%  lager:notice("bld: ~p, ~p",[Res, As]),
  DataList = decode(DType, Res),
  {As, DataList};
%% bits from bytes
bld(Res, {As, _, Bits}) ->
  DataList = decode(bool_byte, Res),
  BitList = [lists:nth(Bit+1, DataList) || Bit <- Bits],
  {As, BitList}.

decode(bool, Data) ->
  binary_to_list(Data);
decode(bool_byte, Data) ->
  D = [X || <<X:1>> <= Data],
  prepare_byte_list(D);
decode(sint, Data) ->
  [Res || <<Res:8/integer-signed>> <= Data];
decode(usint, Data) ->
  decode(byte, Data);
decode(byte, Data) ->
  [Res || <<Res:8/integer-unsigned>> <= Data];
decode(char, Data) ->
  [Res || <<Res:1/binary>> <= Data];
decode(string, Data) ->
  %% strip null-bytes / control-chars
  L = [binary_to_list(Res) || <<Res:1/binary>> <= Data, Res > <<31>>],
  list_to_binary(lists:concat(L));
decode(int, Data) ->
  [Res || <<Res:16/integer-signed>> <= Data];
decode(d_int, Data) ->
  [Res || <<Res:32/integer-signed>> <= Data];
decode(word, Data) ->
  [Res || <<Res:16/unsigned>> <= Data];
decode(d_word, Data) ->
  [Res || <<Res:32/unsigned>> <= Data];
decode(float, Data) ->
  [Res || <<Res:32/float-signed>> <= Data];
decode(ltime, Data) ->
  [Res || <<Res:64/unsigned>> <= Data];
decode(dt, Data) ->
  lager:warning("dt data ~p",[Data]),
  [decode_dt(D) || <<D:8/binary>> <= Data];
decode(dtl, Data) ->
  [decode_dtl(Year, Month, Day, Hour, Minute, Second, NanoSec)
    || <<Year:16/unsigned, Month:8, Day:8, _DayOfWeek:8, Hour:8, Minute:8, Second:8, NanoSec:32>> <= Data];
decode(_, Data) -> Data.

decode_dt(
    <<Y0:1/binary, Month:1/binary, Day:1/binary, Hour:1/binary, Min:1/binary, Sec:1/binary, Milli:12/bits, _WDay:4>>) ->

  Y = bcd_decode(Y0),
  Year = case Y < 90 of true -> 2000+Y; _ -> 1990+Y end,
  DT =
    {{Year, bcd_decode(Month), bcd_decode(Day)},
      {{bcd_decode(Hour), bcd_decode(Min), bcd_decode(Sec)}, list_to_integer(bcd:decode(Milli))}
    },
  faxe_time:to_ms(DT).

bcd_decode(B) ->
  list_to_integer(bcd:decode(B, 1)).

%%decode_dt(DaysSince, MilliSince) ->
%%  % first 4 bytes: days since 1.1.1992 or is it 1990 ?
%%  % second 4 bytes: milliseconds since 00:00:00.000
%%%%  DateStart = qdate:to_date({{1992,1,1}, {0,0,0}}),
%%  DateStart = qdate:to_date({{1990,1,1}, {0,0,0}}),
%%  Date = qdate:add_days(DaysSince, DateStart),
%%  Timestamp0 = qdate:to_unixtime(Date) * 1000,
%%  Timestamp0 + MilliSince.


decode_dtl(Year, Month, Day, Hour, Minute, Second, NanoSec) ->
  faxe_time:to_ms({{Year, Month, Day}, {{Hour, Minute, Second}, round(NanoSec/1000000)}}).


prepare_byte_list(L) ->
  lists:flatten([lists:reverse(LPart) || LPart <- n_length(L,8)]).

n_length(List,Len) ->
  n_length(lists:reverse(List),[],0,Len).

n_length([],Acc,_,_) -> Acc;
n_length([H|T],Acc,Pos,Max) when Pos==Max ->
  n_length(T,[[H] | Acc],1,Max);
n_length([H|T],[HAcc | TAcc],Pos,Max) ->
  n_length(T,[[H | HAcc] | TAcc],Pos+1,Max);
n_length([H|T],[],Pos,Max) ->
  n_length(T,[[H]],Pos+1,Max).