%%%-------------------------------------------------------------------
%%% @author heyoka
%%% @copyright (C) 2022, <COMPANY>
%%% @doc
%%% @todo store clients/addresses in ets and (re)pick these up on startup, ...
%%% @todo ... this way the client does not have to take care of the process going down
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(s7reader).

-behaviour(gen_server).

-export([start_link/1, register/3]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
  code_change/3]).

-define(SERVER, ?MODULE).

-include("faxe.hrl").

-record(state, {
  s7_ip                           :: binary(),
  port              = 102         :: non_neg_integer(),
  conn_opts         = #{}         :: map(),
  %% list of clients with their requested addresses and desired read interval
  clients           = []          :: list(),
  %% all addresses by read interval
  current_addresses = #{}         :: map(),
  %% ready built (optimized) addresses / cache
  request_cache     = #{}         :: map(),
  %% timers for every client read interval
  slot_timers       = []          :: list(),
  %% busy flag
  busy              = false       :: true|false,
  connected         = false       :: true|false,
  %% protocol PDU size, will be fetched from the slave, once connected
  pdu_size          = 240         :: non_neg_integer(),
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
    [] -> {ok, ServerPid} = s7reader_sup:start_reader(Opts), ServerPid;
    [C] -> C %lager:notice("child found: ~p",[C]), C
  end,
%%  lager:notice("So ~p wants to read: ~p every ~pms from ~p",[self(), Vars, Interval, Ip]),
  Server ! {register, Interval, Vars, self()}.

start_link(Opts) ->
  gen_server:start_link(?MODULE, Opts, []).

init(Opts=#{ip := Ip}) ->
  s7pool_manager:connect(Opts),
  {ok, #state{s7_ip = Ip, conn_opts = Opts}}.

handle_call(_Request, _From, State = #state{}) ->
  {reply, ok, State}.

handle_cast(_Request, State = #state{}) ->
  {noreply, State}.

handle_info({read, Requests, [{_Intv, #faxe_timer{last_time = Ts}}]=SendTimers},
    State=#state{slot_timers = SlotTimers, conn_opts = Opts, s7_ip = Ip}) ->
  lager:warning("read ~p requests ", [length(Requests)]),
  ElFun =
    fun({Vars, Aliases}) ->
      case s7pool_manager:read_vars(Opts, Vars) of
        {ok, Res} -> handle_result(Res, Aliases);
        _ -> []
      end
    end,
  RunWith = s7pool_manager:connection_count(Ip),
%%    1,%faxe_config:get(s7pool_max_size, 2),
  {Time, [FirstResult |RequestResults]} = timer:tc(plists, map, [ElFun, Requests, {processes, RunWith}]),
%%  {Time, [FirstResult |RequestResults]} = timer:tc(lists, map, [ElFun, Requests]),
  lager:alert("Time to read: ~p millis with ~p processes/connections",[erlang:round(Time/1000), RunWith]),
  MergeFun =
  fun
    (Prev, Val) when is_map(Prev), is_map(Val) -> maps:merge(Prev, Val);
    (Prev, Val) when is_list(Prev), is_list(Val) -> Prev++Val;
    (_, Val) -> Val
  end,
  TheResult = mapz:deep_merge(MergeFun, FirstResult, RequestResults),

  maps:map(fun(ClientPid, Values) ->
    ClientPid ! {s7_data, Ts, Values}
           end, TheResult),

  %% progress timers used
  NewSlotTimers =
    lists:foldl(
      fun({IntervalKey, STimer = #faxe_timer{last_time = LastTs, interval = Interval}}, Timers) ->
        T = {IntervalKey, STimer#faxe_timer{last_time = LastTs+Interval}},
        lists:keyreplace(IntervalKey, 1, Timers, T)
      end,
      SlotTimers,
      SendTimers),
%%  lager:warning("new slot timers ~p" ,[NewSlotTimers]),
  NewState = State#state{busy = false, slot_timers = NewSlotTimers},
  maybe_next(NewState);

handle_info(try_read, State) ->
  maybe_next(State);
handle_info({register, Interval, Vars, ClientPid},
    State = #state{clients = Clients, current_addresses = CurrentSlots}) ->
  lager:notice("{client: ~p register, ~p}", [ClientPid, {Interval, length(Vars), ClientPid}]),
  erlang:monitor(process, ClientPid),
  NewClient = {ClientPid, Interval, Vars},
  NewClients = Clients++[NewClient],
  Current = maps:get(Interval, CurrentSlots, []),
  NewCurrent = s7_utils:add_client_addresses(ClientPid, Vars, Current),
%%  {T, _BuiltRequests} = timer:tc(s7_utils, build_addresses, [NewCurrent, 240]),
%%  lager:alert("Time to build ~p addresses: ~p", [length(NewCurrent), T]),
%%  _Requests = s7_utils:build_addresses(NewCurrent, 240),
%%  lager:alert("Byte-Size now: ~p",[s7_utils:bit_count(Parts)/8]),
%%  lager:warning("BUILT ~p REQUESTS", [length(Requests)]),
%%  [lager:info("~nREQUEST: ~p ",[Request]) || Request <- Requests],
%%  lager:info("~naddress parts: ~p ~n AliasesList: ~p",[Parts, AliasesList]),
%%  lager:notice("new addresses for: ~p :: ~p", [Interval, NewCurrent]),
  NewSlots = CurrentSlots#{Interval => NewCurrent},
  {noreply, State#state{clients = NewClients, current_addresses = NewSlots, request_cache = #{}}};
handle_info({'DOWN', _MonitorRef, process, Client, _Info}, State = #state{}) ->
  lager:notice("Client: ~p is DOWN",[Client]),
  NewState = remove_client(Client, State),
%%  lager:notice("NEW addresses after deleting client :~p  ~p", [Client, NewAddressSlots]),
  {noreply, NewState};
handle_info({s7_connected, _Client}=M, State = #state{s7_ip = Ip}) ->
  lager:warning("~p s7_connected",[?MODULE]),
  {ok, PduSize} = s7pool_manager:get_pdu_size(Ip),
  lager:notice("pdu size is ~p bytes",[PduSize]),
  send_all_clients(M, State),
  maybe_next(State#state{pdu_size = PduSize});
handle_info({s7_disconnected, _Client}=M, State = #state{timer = Timer}) ->
  lager:alert("~p s7_disconnected",[?MODULE]),
  catch erlang:cancel_timer(Timer),
  send_all_clients(M, State),
  {noreply, State#state{timer = undefined, busy = false}};
handle_info(Info, State = #state{}) ->
  lager:notice("~p got info: ~p",[?MODULE, Info]),
  {noreply, State}.

terminate(Reason, _State = #state{s7_ip = Ip}) ->
  lager:notice("~p for IP ~p stopping with Reason ~p", [?MODULE, Ip, Reason]),
  ok.

code_change(_OldVsn, State = #state{}, _Extra) ->
  {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
send_all_clients(Msg, #state{clients = Clients}) ->
  lists:foreach(fun({ClientPid, _, _}) -> ClientPid ! Msg end, Clients).

maybe_next(State = #state{busy = true}) ->
  {noreply, State};
maybe_next(State = #state{current_addresses = Slots}) when map_size(Slots) == 0 ->
  erlang:send_after(200, self(), try_read),
  {noreply, State#state{busy = false}};
maybe_next(State = #state{busy = false, current_addresses = Slots}) ->
  NewSlotTimers = check_slot_timers(Slots, State#state.slot_timers),
  [{_I, #faxe_timer{last_time = At}}|_] = ReadIntervals = next_read(NewSlotTimers),
%%  lager:notice("NEXT~n SlotTimers:~p~n read-intervals: ~p",[NewSlotTimers, ReadIntervals]),
  Intervals = proplists:get_keys(ReadIntervals),
  {ReadRequests, NewState} = get_requests(Intervals, State),
%%  {Parts, AliasesList} = s7_utils:build_addresses(ReadVars, PDUSize),
%%  lager:alert("Byte-Size now: ~p",[s7_utils:bit_count(Parts)/8]),
%%  lager:info("~naddress parts: ~p ~n AliasesList: ~p",[Parts, AliasesList]),
%%  NewTimer = faxe_time:send_at(At, {read, Parts, AliasesList, ReadIntervals}),
  NewTimer = faxe_time:send_at(At, {read, ReadRequests, ReadIntervals}),
  {noreply, NewState#state{busy = true, slot_timers = NewSlotTimers, timer = NewTimer}}.


check_slot_timers(Slots, CurrentTimers) ->
  SlotIntervals = maps:keys(Slots),
  lists:foldl(
    fun(SlotInterval, Timers) ->
      case lists:keymember(SlotInterval, 1, Timers) of
        true -> Timers;
        false ->
          lager:notice("ADD SLOT : ~p",[SlotInterval]),
          NewTimer0=#faxe_timer{last_time = LastTime, interval = Interval} = faxe_time:timer_new(SlotInterval, true, read),
          %% init timers on the NEXT interval
          [{SlotInterval, NewTimer0#faxe_timer{last_time = LastTime+Interval}}|Timers]
      end
    end,
    CurrentTimers,
    SlotIntervals
  ).

next_read(SlotTimers) ->
  [{_I, #faxe_timer{last_time = NextTs}}|_] =
    lists:usort(fun({_IntervalA, #faxe_timer{last_time = A}}, {_IntervalB, #faxe_timer{last_time = B}}) -> A =< B end,
      SlotTimers),
  FunProceed =
    fun({_Interval, #faxe_timer{last_time = LastTime}} = SlotT, ReadIntervals) ->
%%      lager:info("lasttime: ~p vs nextts: ~p for: ~p",[LastTime, NextTs, Interval]),
      case LastTime == NextTs of
        true -> [SlotT|ReadIntervals];
        false -> ReadIntervals
      end
    end,
  NextSlotsToRead = lists:foldl(FunProceed, [], SlotTimers),
%%  lager:warning("Now: ~p NextReads: ~p", [faxe_time:now(), Reads]),
%%  lager:warning("NextSlotsToRead: ~p", [NextSlotsToRead]),
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
  {T, BuiltRequests} = timer:tc(s7_utils, build_addresses, [Addresses, PDUSize]),
  lager:alert("Time to build ~p addresses: ~p", [length(Addresses), T]),
  {BuiltRequests, S#state{request_cache = Cache#{IntervalList => BuiltRequests}}}.


remove_client(Client, State = #state{clients = Clients, current_addresses = AddressSlots, slot_timers = STimers}) ->
  {NewAddressSlots, NewClients, SlotTimers} =
  case lists:keytake(Client, 1, Clients) of
    {value, {Client, Interval, Vars}, Clients2} ->
      case catch maps:take(Interval, AddressSlots) of
        {Addresses, RemainingSlots} when is_list(Addresses) ->
          %% remove client from clients and addresses
          Removed = s7_utils:remove_client(Client, Vars, Addresses),
%%          lager:info("after removing addresses are: ~p",[Removed]),
          case Removed of
            [] ->
              %% no more clients/vars left for this timeslot, so we delete the slot-timer also
              {RemainingSlots, Clients2, proplists:delete(Interval, STimers)};
            Res when is_list(Res) -> {RemainingSlots#{Interval => Res}, Clients2, STimers}
          end;
        _ ->
          {AddressSlots, Clients2, STimers}
      end;
    false ->
      {AddressSlots, Clients, STimers}
  end,
  State#state{clients = NewClients, current_addresses = NewAddressSlots, slot_timers = SlotTimers, request_cache = #{}}.

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
%%  lager:notice("bld string single: ~p, ~p",[Res, As]),
  Data = decode(string, Res),
  {[As], [Data]};
%% non-bool, non-single, string
bld(Res, {As, [DType|_]}) ->
%%  lager:notice("bld: ~p, ~p",[Res, As]),
  DataList = decode(DType, Res),
  {As, DataList};
%% bits from bytes
bld(Res, {As, _, Bits}) ->
%%  lager:notice("bld bool: ~p, ~p",[Res, As]),
  DataList = decode(bool_byte, Res),
  BitList = [lists:nth(Bit+1, DataList) || Bit <- Bits],
  {As, BitList}.

decode(bool, Data) ->
  binary_to_list(Data);
decode(bool_byte, Data) ->
  D = [X || <<X:1>> <= Data],
  prepare_byte_list(D);
decode(byte, Data) ->
  [Res || <<Res:8/integer-unsigned>> <= Data];
decode(char, Data) ->
  [Res || <<Res:1/binary>> <= Data];
decode(string, Data) ->
  %% strip null-bytes / control-chars
%%  L = [binary_to_list(Res) || <<Res:1/binary>> <= Data, Res /= <<0>>],
  L = [binary_to_list(Res) || <<Res:1/binary>> <= Data, Res > <<31>>],
%%  lager:info("~p",[L]),
  list_to_binary(lists:concat(L));
decode(int, Data) ->
  [Res || <<Res:16/integer-signed>> <= Data];
decode(d_int, Data) ->
  [Res || <<Res:32/integer-signed>> <= Data];
decode(word, Data) ->
  [Res || <<Res:16/unsigned>> <= Data];
decode(d_word, Data) ->
  [Res || <<Res:32/float-unsigned>> <= Data];
decode(float, Data) ->
  [Res || <<Res:32/float-signed>> <= Data];
decode(_, Data) -> Data.

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