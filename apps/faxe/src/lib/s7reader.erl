%%%-------------------------------------------------------------------
%%% @author heyoka
%%% @copyright (C) 2022, <COMPANY>
%%% @doc
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(s7reader).

-behaviour(gen_server).

-export([start_link/1, register/3]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
  code_change/3]).

-define(SERVER, ?MODULE).
-define(EMPTY_RETRY_INTERVAL, 300).

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
    [C] when is_pid(C) ->
      C;
    [undefined] ->
      %% the child pid is undefined, when it has stopped previously, so we simply restart it
      {ok, Child} = supervisor:restart_child(s7reader_sup, Ip),
      Child;
    _ ->
      {ok, ServerPid} = s7reader_sup:start_reader(Opts),
      ServerPid
end,
%%  lager:notice("So ~p wants to read: ~p every ~pms from ~p",[self(), Vars, Interval, Ip]),
  Server ! {register, Interval, Vars, self()}.

start_link(Opts) ->
  gen_server:start_link(?MODULE, Opts, []).

init(Opts0=#{ip := Ip}) ->
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


handle_info({read, Requests, [{_Intv, #faxe_timer{last_time = Ts}}] = SendTimers},
    State=#state{slot_timers = SlotTimers, conn_opts = Opts, s7_ip = Ip}) ->
%%  lager:warning("read ~p requests ", [length(Requests)]),
  ElFun =
    fun({Vars, Aliases}) ->
      case s7pool_manager:read_vars(Opts, Vars) of
        {ok, Res} -> handle_result(Res, Aliases);
        _ -> []
      end
    end,
  RunWith = s7pool_manager:connection_count(Ip),
%%  lager:notice("s7reader:read - connection count is ~p",[RunWith]),
  case RunWith > 0 of
    true ->
      %%    1,%faxe_config:get(s7pool_max_size, 2),
      {Time, [FirstResult |RequestResults]} = timer:tc(plists, map, [ElFun, Requests, {processes, RunWith}]),
%%  {Time, [FirstResult |RequestResults]} = timer:tc(lists, map, [ElFun, Requests]),

      lager:notice("Time to read ~p requests: ~p millis with ~p processes/connections",
        [length(Requests), erlang:round(Time/1000), RunWith]),
      MergeFun =
        fun
          (Prev, Val) when is_map(Prev), is_map(Val) -> maps:merge(Prev, Val);
          (Prev, Val) when is_list(Prev), is_list(Val) -> Prev++Val;
          (_, Val) -> Val
        end,
      TheResult = mapz:deep_merge(MergeFun, FirstResult, RequestResults),

      maps:map(fun(ClientPid, Values) ->
        ClientPid ! {s7_data, Ts, Values}
               end, TheResult);
    false ->
      lager:warning("~p no connections available!",[?MODULE]),
      ok
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
%%  lager:warning("new slot timers ~p" ,[NewSlotTimers]),
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
  lager:notice("Client: ~p is DOWN",[Client]),
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
  lager:notice("pdu size is ~p bytes",[PduSize]),
  send_all_clients(M, State),
  %% clear the cache, if pdu size changed
  NewCache =
  case CachePDU == PduSize of
    true -> Cache;
    false -> lager:info("s7reader clear cache on connect, cause PDU size changed to ~p",[PduSize]), #{}
  end,
  maybe_next(State#state{pdu_size = PduSize, connected = true, retrieve_pdu_size = Retrieve, request_cache = NewCache});
handle_info({s7_disconnected, _Client}=M, State = #state{timer = Timer}) ->
  catch erlang:cancel_timer(Timer),
  send_all_clients(M, State),
  {noreply, State#state{timer = undefined, busy = false, connected = false}};
handle_info(Info, State = #state{}) ->
  lager:info("~p got info: ~p",[?MODULE, Info]),
  maybe_next(State).

terminate(Reason, _State = #state{s7_ip = Ip}) ->
  lager:notice("~p for IP ~p stopping with Reason ~p", [?MODULE, Ip, Reason]),
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
  lager:notice("maybe_next when not connected"),
  {noreply, State};
maybe_next(State = #state{busy = true}) ->
  {noreply, State};
maybe_next(State = #state{current_addresses = Slots}) when map_size(Slots) == 0 ->
  erlang:send_after(?EMPTY_RETRY_INTERVAL, self(), try_read),
  {noreply, State#state{busy = false}};
maybe_next(State = #state{busy = false, current_addresses = Slots}) ->
  NewSlotTimers = check_slot_timers(Slots, State#state.slot_timers),
  [{_I, #faxe_timer{last_time = At}}|_] = ReadIntervals = next_read(NewSlotTimers),
%%  lager:notice("NEXT~n SlotTimers:~p~n read-intervals: ~p",[NewSlotTimers, ReadIntervals]),
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
bld(Res, {As, _, Bits}=T) ->
%%  lager:notice("~p bld bool: ~p, ~p",[?MODULE, Res, T]),
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