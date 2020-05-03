%%%-------------------------------------------------------------------
%%% @author heyoka
%%% @copyright (C) 2019, <COMPANY>
%%% @doc
%%% get data from a siemens s7 plc via the snap7 library
%%%
%%% @end
%%% Created : 14. June 2019 11:32:22
%%%-------------------------------------------------------------------
-module(esp_s7read).
-author("heyoka").

%% API
-behavior(df_component).

-include("faxe.hrl").
%% API
-export([init/3, process/3, options/0, handle_info/2, shutdown/1, maybe_emit/5,
  check_options/0, build_addresses/2, build_point/2, test_build/0]).

-define(MAX_READ_ITEMS, 19).

-define(RECON_MIN_INTERVAL, 100).
-define(RECON_MAX_INTERVAL, 6000).
-define(RECON_MAX_RETRIES, infinity).

-record(state, {
  ip,
  port,
  client,
  slot,
  rack,
  interval,
  as,
  align,
  diff,
  timer_ref,
  vars, var_types :: list(),
  last_values = [] :: list(),
  timer :: #faxe_timer{},
  reconnector
}).

options() -> [
  {ip, binary},
  {port, integer, 102},
  {every, duration, <<"1s">>},
  {align, is_set},
  {slot, integer, 1},
  {rack, integer, 0},
  {vars, string_list}, %% s7 addressing, ie: DB2024,Int16.1224 | DB2024.DBX12.2
  {as, binary_list},
  {diff, is_set}].

check_options() ->
  [
    {func, vars,
      fun(List) -> {P, _} = build_addresses(List, lists:seq(1, length(List))),
        length(P) =< ?MAX_READ_ITEMS end,
      <<", has to many address items">>
    }
  ].

init(_NodeId, _Ins,
    #{ip := Ip,
      port := Port,
      every := Dur,
      align := Align,
      slot := Slot,
      rack := Rack,
      vars := Addresses,
      as := As,
      diff := Diff}) ->

  Reconnector = faxe_backoff:new(
    {?RECON_MIN_INTERVAL, ?RECON_MAX_INTERVAL, ?RECON_MAX_RETRIES}),
  {Parts, AliasesList} = build_addresses(Addresses, As),

  lager:info("~p VARS reduced to : ~p",[length(Addresses), length(Parts)]),
  [lager:notice("Partition: ~p", [Part]) || Part <- Parts],
%%  erlang:send_after(0, self(), do_reconnect),

  {ok, all,
    #state{
      ip = Ip,
      port = Port,
      as = AliasesList,
      slot = Slot,
      rack = Rack,
      align = Align,
      interval = Dur,
      reconnector = Reconnector,
      diff = Diff,
      vars = Parts}
  }.


process(_In, #data_batch{points = _Points} = _Batch, State = #state{}) ->
  {ok, State};
process(_Inport, #data_point{} = _Point, State = #state{}) ->
  {ok, State}.

handle_info(poll,
    State=#state{client = Client, as = Aliases, timer = Timer,
      vars = Opts, diff = Diff, last_values = LastList}) ->
  lager:notice("opts  are: ~p",[Opts]),
  case (catch snapclient:read_multi_vars(Client, Opts)) of
    {ok, Res} ->
      {ok, ExecTime} = snapclient:get_exec_time(Client),
      lager:notice("got data form s7 in: ~pms ~n~p", [ExecTime, Res]),
      NewTimer = faxe_time:timer_next(Timer),
      NewState = State#state{timer = NewTimer, last_values = Res},
      maybe_emit(Diff, Res, Aliases, LastList, NewState);
    _Other ->
      lager:warning("Error when reading S7 Vars: ~p", [_Other]),
      NewTimer = faxe_time:timer_cancel(Timer),
      try_reconnect(State#state{client = undefined, timer = NewTimer})
  end;
%% client process is down,
%% we match the Object field from the DOWN message against the current client pid
handle_info({'DOWN', _MonitorRef, _Type, Client, Info},
    State=#state{client = Client, timer = Timer}) ->
  lager:warning("Snap7 Client process is DOWN with : ~p ! ", [Info]),
  NewTimer = faxe_time:timer_cancel(Timer),
  try_reconnect(State#state{client = undefined, timer = NewTimer});
%% old DOWN message from already restarted client process
handle_info({'DOWN', _MonitorRef, _Type, _Object, _Info}, State) ->
  {ok, State};
handle_info(do_reconnect,
    State=#state{ip = Ip, port = Port, rack = Rack, slot = Slot, align = Align, interval = Dur}) ->
  lager:info("[~p] do_reconnect, ~p", [?MODULE, {Ip, Port}]),
  case connect(Ip, Rack, Slot) of
    {ok, Client} ->
      Timer = faxe_time:init_timer(Align, Dur, poll),
      {ok, State#state{client = Client, timer = Timer}};
    {error, Error} ->
      lager:error("[~p] Error connecting to PLC ~p: ~p",[?MODULE, {Ip, Port},Error]),
      try_reconnect(State)
  end;
handle_info(_E, S) ->
  {ok, S#state{}}.

shutdown(#state{client = Client, timer = Timer}) ->
  catch (faxe_time:timer_cancel(Timer)),
  catch (snapclient:disconnect(Client)),
  catch (snapclient:stop(Client)).

try_reconnect(State=#state{reconnector = Reconnector}) ->
  case faxe_backoff:execute(Reconnector, do_reconnect) of
    {ok, Reconnector1} ->
      {ok, State#state{reconnector = Reconnector1}};
    {stop, Error} -> logger:error("[Client: ~p] PLC reconnect error: ~p!",[?MODULE, Error]),
      {stop, {shutdown, Error}, State}
  end.

-spec maybe_emit(Diff :: true|false, ResultList :: list(), Aliases :: list(), LastResults :: list(), State :: #state{})
      -> ok | term().
%% no diff flag -> emit
maybe_emit(false, Res, Aliases, _, State) ->
  Out = build_point(Res, Aliases),
  {emit, {1, Out}, State};
%% no last values -> emit
maybe_emit(true, Res, Aliases, [], State) ->
  maybe_emit(false, Res, Aliases, [], State);
%% diff flag and result-list is exactly last list -> no emit
maybe_emit(true, Result, _, Result, State) ->
%%  lager:warning("diff is true and there is no diff in ResultList !"),
  {ok, State};
%% diff flag -> emit diff values only
maybe_emit(true, Result, Aliases, LastList, State) ->
  ResAliasList = lists:zip(Aliases, Result),
  LastAliasList = lists:zip(Aliases, LastList),
%%  lager:info("resalias: ~p ~n lastalias : ~p",[ResAliasList, LastAliasList]),
  ResList = lists:filter(
    fun({K, Res}) -> (proplists:get_value(K, LastAliasList) /= Res) end,
    ResAliasList),
  {ResAliases, ResValues} = lists:unzip(ResList),
%%  lager:info("unzipped again: ~p" ,[lists:unzip(ResList)]),
  Out = build_point(ResValues, ResAliases),
  {emit, {1, Out}, State}.

build_addresses(Addresses, As) ->
  PList = [s7addr:parse(Address) || Address <- Addresses],
  %% inject Aliases into parameter maps
  AsAdds = lists:zip(As, PList),
  F = fun({Alias, Params}) -> Params#{as => Alias} end,
  WithAs = lists:map(F, AsAdds),
  PartitionFun =
    fun(#{dtype := Dtype} = E, Acc) ->
      case maps:is_key(Dtype, Acc) of
        true -> Acc#{Dtype => [E|maps:get(Dtype, Acc)]};
        false -> Acc#{Dtype => [E]}
      end
    end,
  Splitted = lists:foldl(PartitionFun, #{}, WithAs),
%%  [lager:notice("Splitted: ~p", [S]) || S <- maps:to_list(Splitted)],
  %% sort by starts
  ParamsSorted =
    lists:flatten(
      lists:map(fun({_Type, L}) -> sort_by_start(L) end, maps:to_list(Splitted))
    ),
%%  [lager:notice("Flattened Sorted: ~p", [S]) || S <- ParamsSorted],
  %% find contiguous starts
  find_contiguous(ParamsSorted).

%% sort a list of parsed s7 address maps by (db_number+)start
sort_by_start(ParamList) ->
  lists:sort(
    fun(#{start := StartA, db_number := DbA}, #{start := StartB, db_number := DbB}) ->
      %% we multiple the db_number by 10000 to avoid getting db-start addresses mixed up accidentally
      (DbA*10000 + StartA) < (DbB*10000 + StartB) end,
    ParamList).

find_contiguous(ParamList) ->
  F = fun(#{start := Start, as := As, db_number := DB, dtype := DType} = E,
      {LastStart, Current = #{aliases := CAs, amount := CAmount, db_number := CDB, dtype := CType}, Partitions}) ->
    case (DType == CType) andalso (DB == CDB) andalso (LastStart + word_len_size(DType) == Start) of
      true ->
        NewCurrent = Current#{amount => CAmount+1, aliases => CAs++[{As, DType}]},
        {Start, NewCurrent, Partitions};
      false ->
        {Start, E#{aliases => [{As, DType}]}, Partitions++[Current]}
    end
      end,
  {_Last, Current, [_|Parts]} =
    lists:foldl(F, {-2, #{aliases => [], amount => 0, db_number => -1, dtype => nil}, []}, ParamList),
  All = [Current|Parts],
  FAs = fun(#{aliases := Aliases}) -> Aliases end,
  AliasesList = lists:map(FAs, All),
  AddressPartitions = [maps:without([aliases, as, dtype], M) || M <- All],
  {AddressPartitions, AliasesList}.

word_len_size(bool) -> 1;
word_len_size(byte) -> 1;
word_len_size(char) -> 1;
word_len_size(word) -> 2;
word_len_size(int) -> 1;
word_len_size(d_word) -> 4;
word_len_size(d_int) -> 4;
word_len_size(float) -> 4;
word_len_size(timer) -> 4;
word_len_size(counter) -> 4.

bit_count(VarList) ->
  bit_count(VarList, 0).
bit_count([], Acc) ->
  Acc;
bit_count([#{word_len := bit}|Rest], Acc) ->
  bit_count(Rest, Acc + 1);
bit_count([#{word_len := byte}|Rest], Acc) ->
  bit_count(Rest, Acc + 8);
bit_count([#{word_len := word}|Rest], Acc) ->
  bit_count(Rest, Acc + 16);
bit_count([#{word_len := d_word}|Rest], Acc) ->
  bit_count(Rest, Acc + 32);
bit_count([#{word_len := real}|Rest], Acc) ->
  bit_count(Rest, Acc + 32).


build_point(ResultList, AliasesList) when is_list(ResultList), is_list(AliasesList) ->
  lager:notice("RESULT: ~p~n",[lists:zip(ResultList, AliasesList)]),
  do_build(#data_point{ts=faxe_time:now()}, ResultList, AliasesList).

do_build(Point=#data_point{}, [], []) ->
  Point;
do_build(Point=#data_point{}, [Res|R], [Aliases|AliasesList]) ->
  {As, [DType|_]} = lists:unzip(Aliases),
  DataList = decode(DType, Res),
  lager:notice("DataList: ~p",[DataList]),
  NewPoint = flowdata:set_fields(Point, As, DataList),
  do_build(NewPoint, R, AliasesList).

decode(bool, Data) ->
  binary_to_list(Data);
%%  [X || <<X:1/bits>> <= Data];
decode(byte, Data) ->
  [Res || <<Res:8/binary>> <= Data];
decode(char, Data) ->
  [Res || <<Res:1/binary>> <= Data];
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


connect(Ip, Rack, Slot) ->
  case catch do_connect(Ip, Rack, Slot) of
    Client when is_pid(Client) -> {ok, Client};
    Err -> {error, Err}
  end.

do_connect(Ip, Rack, Slot) ->
  {ok, Client} = snapclient:start([]),
  ok = snapclient:connect_to(Client, [{ip, Ip}, {slot, Slot}, {rack, Rack}]),
  {ok, CPUInfo} = snapclient:get_cpu_info(Client),
  lager:warning("Connected to PLC : ~p",[CPUInfo]),
  {ok, NegotiatedLength} = snapclient:get_pdu_length(Client),
  lager:warning("PDU-Length is : ~p",[NegotiatedLength]),
  erlang:monitor(process, Client),
  Client.


test_build() ->
  Data = list_to_binary([0,0,0,1,1,0,0]),
  lager:info("binary data: ~p",[{Data, binary_to_list(Data)}]),
  decode(bool, Data).