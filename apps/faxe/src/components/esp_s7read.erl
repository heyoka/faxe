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
%%  check_options/0,
  build_addresses/2, build_point/2, do_build/3, check_pdu_length/3, n_length/2, check_options/0]).

-define(MAX_READ_ITEMS, 19).
-define(DEFAULT_BYTE_LIMIT, 128).

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
      fun(List) ->
        {P, _} = build_addresses(List, lists:seq(1, length(List))),
        length(P) =< ?MAX_READ_ITEMS
      end,
      <<", has to many address items!">>
    },
    {func, vars,
      fun(List) ->
        {P, _} = build_addresses(List, lists:seq(1, length(List))),
        bit_count(P)/8 =< ?DEFAULT_BYTE_LIMIT
      end,
      <<", byte-size ", ?DEFAULT_BYTE_LIMIT, " bytes exceeded!">>
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

%%  lager:info("~p VARS reduced to : ~p  with byte-size: ~p",[length(Addresses), length(Parts), bit_count(Parts)/8]),
%%  [lager:notice("Partition: ~p", [Part]) || Part <- Parts],
%%  [lager:notice("Aliases: ~p", [Part]) || Part <- AliasesList],

  erlang:send_after(0, self(), do_reconnect),
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

handle_info({snap7_connected, Client}, State = #state{align = Align, interval = Dur, client = Client}) ->
  lager:notice("snap7_connected: ~p", [Client]),
  Timer = faxe_time:init_timer(Align, Dur, poll),
  {ok, State#state{client = Client, timer = Timer}};
handle_info(poll,
    State=#state{client = Client, as = Aliases, timer = Timer,
      vars = Opts, diff = Diff, last_values = LastList}) ->
  case (catch snapclient:read_multi_vars(Client, Opts)) of
    {ok, Res} ->
%%      {ok, ExecTime} = snapclient:get_exec_time(Client),
      lager:notice("got data form s7 ~n~p", [Res]),
      NewTimer = faxe_time:timer_next(Timer),
      NewState = State#state{timer = NewTimer, last_values = Res},
      maybe_emit(Diff, Res, Aliases, LastList, NewState);
    _Other ->
      lager:warning("Error when reading S7 Vars: ~p", [_Other]),
      NewTimer = faxe_time:timer_cancel(Timer),
      catch snapclient:stop(Client),
      %% keep client in state, stop will trigger the DOWN message below
      {ok, State#state{timer = NewTimer}}
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
    State=#state{ip = Ip, port = Port, rack = Rack, slot = Slot}) ->
  lager:info("[~p] do_reconnect, ~p", [?MODULE, {Ip, Port}]),
  case connect(Ip, Rack, Slot) of
    {ok, Client} ->
      {ok, State#state{client = Client}};
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
%% diff flag and result-list is exactly last list -> no emit
maybe_emit(true, Result, _, Result, State) ->
  {ok, State};
%% diff flag -> emit values
maybe_emit(true, Result, Aliases, _Last, State) ->
  maybe_emit(false, Result, Aliases, [], State).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
build_addresses(Addresses, As) ->
  PList = [s7addr:parse(Address) || Address <- Addresses],
  lager:notice("Addresses: ~p",[PList]),
  %% inject Aliases into parameter maps
  AsAdds = lists:zip(As, PList),
  F = fun({Alias, Params}) -> Params#{as => Alias} end,
  WithAs = lists:map(F, AsAdds),
  %% partition addresses+aliases by data-type
  PartitionFun =
    fun(#{dtype := Dtype, start := Start} = E, Acc) ->
      lager:notice("DType: ~p, E: ~p",[Dtype, E]),
      Ele =
      case Dtype == bool of
        true -> E#{byte_num => erlang:trunc(Start/8), bit_num => Start rem 8};
        false -> E
      end,
      case maps:is_key(Dtype, Acc) of
        true -> Acc#{Dtype => [Ele|maps:get(Dtype, Acc)]};
        false -> Acc#{Dtype => [Ele]}
      end
    end,
  Splitted = lists:foldl(PartitionFun, #{}, WithAs),
%%  lager:notice("Splitted: ~p",[Splitted]),
  %% extract bit addresses
  {Bools, NonBools} =
  case maps:take(bool, Splitted) of
    error -> {[], Splitted};
    Other -> Other
  end,
  lager:notice("NonBools: ~p",[NonBools]),
  %% sort bit addresses
  BoolsSorted = sort_by_start(Bools),
  %% build byte addresses for bits
  {BoolParts, BoolAliases} = find_bool_bytes(BoolsSorted),

  %% sort by starts
  ParamsSorted = lists:flatmap(fun({_Type, L}) -> sort_by_start(L) end, maps:to_list(NonBools)),
  %% find contiguous starts
  {NonBoolParts, NonBoolAliases} = find_contiguous(ParamsSorted),
  {BoolParts ++ NonBoolParts, BoolAliases ++ NonBoolAliases}.

-spec find_bool_bytes(list()) -> tuple().
find_bool_bytes([]) -> {[],[]};
find_bool_bytes(Bools) ->
  CFun = fun(#{byte_num := Byte, db_number := DB, as := As, bit_num := Bit} = E,
      {LastByte, Current = #{aliases := CAs, db_number := CDB, amount := CAmount, start := CStartByte}, Partitions}) ->
    case (DB == CDB) andalso (LastByte == Byte orelse (Byte == LastByte + 1 andalso Bit == 0)) of
      true ->
        NewCurrent0 = Current#{aliases => CAs++[{As, bool_byte, (Bit+(Byte-CStartByte)*8)}]},
        NewCurrent =
        case LastByte + 1 == Byte of
          true -> NewCurrent0#{amount => CAmount+1};
          false -> NewCurrent0
        end,
        {Byte, NewCurrent, Partitions};
      false ->
        {Byte,
          E#{amount => 1, start => Byte, word_len => byte,
            aliases => [{As, bool_byte, Bit}]}, Partitions++[Current]}
    end
         end,
  {_Last, Current, [_|Parts]} =
    lists:foldl(CFun, {-2, #{aliases => [], amount => 0, db_number => -1, start => -2}, []}, Bools),

  All = Parts ++ [Current],
  FAs = fun(#{aliases := Aliases}) -> lists:unzip3(Aliases) end,
  AliasesList = lists:map(FAs, All),
  AddressPartitions = [maps:without([aliases, as, dtype, byte_num, bit_num], M) || M <- All],
  {AddressPartitions, AliasesList}
.


%% sort a list of parsed s7 address maps by (db_number+)start
sort_by_start(ParamList) ->
  lists:sort(
    fun(#{start := StartA, db_number := DbA}, #{start := StartB, db_number := DbB}) ->
      %% we multiple the db_number by 10000 to avoid getting db-start addresses mixed up accidentally
      (DbA*10000 + StartA) < (DbB*10000 + StartB) end,
    ParamList).


-spec find_contiguous(list()) -> tuple().
find_contiguous([]) -> {[], []};
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
  FAs = fun(#{aliases := Aliases}) -> lists:unzip(Aliases) end,
  AliasesList = lists:map(FAs, All),
  AddressPartitions = [maps:without([aliases, as, dtype], M) || M <- All],
  {AddressPartitions, AliasesList}.

word_len_size(bool) -> 1;
word_len_size(byte) -> 1;
word_len_size(char) -> 1;
word_len_size(string) -> 1;
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
bit_count([#{word_len := bit, amount := Amount}|Rest], Acc) ->
  bit_count(Rest, Acc + 1*Amount);
bit_count([#{word_len := byte, amount := Amount}|Rest], Acc) ->
  bit_count(Rest, Acc + 8*Amount);
bit_count([#{word_len := word, amount := Amount}|Rest], Acc) ->
  bit_count(Rest, Acc + 16*Amount);
bit_count([#{word_len := d_word, amount := Amount}|Rest], Acc) ->
  bit_count(Rest, Acc + 32*Amount);
bit_count([#{word_len := real, amount := Amount}|Rest], Acc) ->
  bit_count(Rest, Acc + 32*Amount).


build_point(ResultList, AliasesList) when is_list(ResultList), is_list(AliasesList) ->
  do_build(#data_point{ts=faxe_time:now()}, ResultList, AliasesList).

do_build(Point=#data_point{}, [], []) ->
  Point;
do_build(Point=#data_point{}, [Res|R], [Aliases|AliasesList]) ->
  {Keys, Values} = bld(Res, Aliases),
  NewPoint = flowdata:set_fields(Point, Keys, Values),
  do_build(NewPoint, R, AliasesList).

%% string is a special case, multiple chars(bytes) form one string
-spec bld(list()|binary(), {list(), list()}) -> {list(), list()}.
bld(Res, {[As], [string]}) ->
  lager:notice("bld string single: ~p, ~p",[Res, As]),
  Data = decode(string, Res),
  {[As], [Data]};
%% non-bool, non-single, string
bld(Res, {As, [DType|_]}) ->
  lager:notice("bld: ~p, ~p",[Res, As]),
  DataList = decode(DType, Res),
  {As, DataList};
%% bits from bytes
bld(Res, {As, _, Bits}) ->
  lager:notice("bld bool: ~p, ~p",[Res, As]),
  DataList = decode(bool_byte, Res),
  BitList = [lists:nth(Bit+1, DataList) || Bit <- Bits],
  {As, BitList}.

decode(bool, Data) ->
  binary_to_list(Data);
decode(bool_byte, Data) ->
  D = [X || <<X:1>> <= Data],
  prepare_byte_list(D);
decode(byte, Data) ->
  [Res || <<Res:8/binary>> <= Data];
decode(char, Data) ->
  [Res || <<Res:1/binary>> <= Data];
decode(string, Data) ->
  %% strip null-bytes / control-chars
%%  L = [binary_to_list(Res) || <<Res:1/binary>> <= Data, Res /= <<0>>],
  L = [binary_to_list(Res) || <<Res:1/binary>> <= Data, Res > <<31>>],
  lager:info("~p",[L]),
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



connect(Ip, Rack, Slot) ->
  case catch do_connect(Ip, Rack, Slot) of
    Client when is_pid(Client) -> {ok, Client};
    Err -> {error, Err}
  end.

do_connect(Ip, Rack, Slot) ->
  {ok, Client} = snapclient:start_connect(#{ip => Ip, rack => Rack, slot => Slot}),
  erlang:monitor(process, Client),
  Client.

check_pdu_length(Ip, Slot, Rack) ->
  try do_connect(Ip, Slot, Rack) of
    Client when is_pid(Client) ->
      {ok, NegotiatedLength} = snapclient:get_pdu_length(Client),
      NegotiatedLength;
    _ ->
      ?DEFAULT_BYTE_LIMIT
  catch
    _ -> ?DEFAULT_BYTE_LIMIT
  end.




-ifdef(TEST).
build_addresses_test() ->
  L = [
    <<"DB11136.DBX88.0">>,<<"DB11136.DBX88.1">>,<<"DB11136.DBX88.2">>,<<"DB11136.DBX88.3">>,
    <<"DB11136.DBX90.0">>,<<"DB11136.DBX90.1">>,<<"DB11136.DBX90.2">>,<<"DB11136.DBX90.3">>,
    <<"DB11136.DBX90.4">>,<<"DB11136.DBX90.5">>,<<"DB11136.DBX90.6">>,<<"DB11136.DBX90.7">>,
    <<"DB11136.DBX91.0">>,<<"DB11136.DBX91.1">>,<<"DB11136.DBX91.2">>,<<"DB11136.DBX91.3">>,
    <<"DB11136.DBX91.7">>,<<"DB11136.DBX92.0">>,<<"DB11136.DBX92.1">>,<<"DB11136.DBX92.2">>,
    <<"DB11136.DBX92.3">>,<<"DB11136.DBX92.4">>,<<"DB11136.DBX92.5">>,<<"DB11136.DBX92.6">>,
    <<"DB11136.DBX92.7">>,<<"DB11136.DBX93.0">>,<<"DB11136.DBX93.1">>,<<"DB11136.DBX93.2">>,
    <<"DB11136.DBX93.3">>,<<"DB11136.DBX93.4">>,<<"DB11136.DBX93.5">>,<<"DB11136.DBX93.6">>,
    <<"DB11136.DBX93.7">>,<<"DB11136.DBX94.0">>,<<"DB11136.DBX94.1">>,<<"DB11136.DBW96">>,
    <<"DB11136.DBW98">>,<<"DB11136.DBX100.0">>,<<"DB11136.DBX100.1">>,<<"DB11136.DBX100.2">>,
    <<"DB11136.DBX100.4">>,<<"DB11136.DBX100.5">>,<<"DB11136.DBX100.6">>,<<"DB11136.DBX100.7">>,
    <<"DB11136.DBX101.0">>,<<"DB11136.DBX101.1">>,<<"DB11136.DBX101.2">>,<<"DB11136.DBX101.3">>,
    <<"DB11136.DBX101.4">>,<<"DB11136.DBX101.6">>,<<"DB11136.DBX101.7">>,<<"DB11136.DBX102.0">>,
    <<"DB11136.DBX102.1">>],
  As = lists:map(fun(E) -> binary:replace(E, <<".">>, <<"_">>, [global]) end, L),
  Res = [
    #{amount => 1,area => db,db_number => 11136,start => 88,word_len => byte},
    #{amount => 5,area => db,db_number => 11136,start => 90,word_len => byte},
    #{amount => 3,area => db,db_number => 11136,start => 100,word_len => byte},
    #{amount => 2,area => db,db_number => 11136,start => 96,word_len => word}
  ],
  AliasesList =
    [
      {[<<"DB11136_DBX88_0">>,<<"DB11136_DBX88_1">>,
      <<"DB11136_DBX88_2">>,<<"DB11136_DBX88_3">>],
      [bool_byte,bool_byte,bool_byte,bool_byte],
      [0,1,2,3]},
      {[<<"DB11136_DBX90_0">>,<<"DB11136_DBX90_1">>,
        <<"DB11136_DBX90_2">>,<<"DB11136_DBX90_3">>,
        <<"DB11136_DBX90_4">>,<<"DB11136_DBX90_5">>,
        <<"DB11136_DBX90_6">>,<<"DB11136_DBX90_7">>,
        <<"DB11136_DBX91_0">>,<<"DB11136_DBX91_1">>,
        <<"DB11136_DBX91_2">>,<<"DB11136_DBX91_3">>,
        <<"DB11136_DBX91_7">>,<<"DB11136_DBX92_0">>,
        <<"DB11136_DBX92_1">>,<<"DB11136_DBX92_2">>,
        <<"DB11136_DBX92_3">>,<<"DB11136_DBX92_4">>,
        <<"DB11136_DBX92_5">>,<<"DB11136_DBX92_6">>,
        <<"DB11136_DBX92_7">>,<<"DB11136_DBX93_0">>,
        <<"DB11136_DBX93_1">>,<<"DB11136_DBX93_2">>,
        <<"DB11136_DBX93_3">>,<<"DB11136_DBX93_4">>,
        <<"DB11136_DBX93_5">>,<<"DB11136_DBX93_6">>,
        <<"DB11136_DBX93_7">>,<<"DB11136_DBX94_0">>,
        <<"DB11136_DBX94_1">>],
        [bool_byte,bool_byte,bool_byte,bool_byte,bool_byte,
          bool_byte,bool_byte,bool_byte,bool_byte,bool_byte,
          bool_byte,bool_byte,bool_byte,bool_byte,bool_byte,
          bool_byte,bool_byte,bool_byte,bool_byte,bool_byte,
          bool_byte,bool_byte,bool_byte,bool_byte,bool_byte,
          bool_byte,bool_byte,bool_byte,bool_byte,bool_byte,
          bool_byte],
        [0,1,2,3,4,5,6,7,8,9,10,11,15,16,17,18,19,20,21,22,23,24,
          25,26,27,28,29,30,31,32,33]},
      {[<<"DB11136_DBX100_0">>,<<"DB11136_DBX100_1">>,
        <<"DB11136_DBX100_2">>,<<"DB11136_DBX100_4">>,
        <<"DB11136_DBX100_5">>,<<"DB11136_DBX100_6">>,
        <<"DB11136_DBX100_7">>,<<"DB11136_DBX101_0">>,
        <<"DB11136_DBX101_1">>,<<"DB11136_DBX101_2">>,
        <<"DB11136_DBX101_3">>,<<"DB11136_DBX101_4">>,
        <<"DB11136_DBX101_6">>,<<"DB11136_DBX101_7">>,
        <<"DB11136_DBX102_0">>,<<"DB11136_DBX102_1">>],
        [bool_byte,bool_byte,bool_byte,bool_byte,bool_byte,
          bool_byte,bool_byte,bool_byte,bool_byte,bool_byte,
          bool_byte,bool_byte,bool_byte,bool_byte,bool_byte,
          bool_byte],
        [0,1,2,4,5,6,7,8,9,10,11,12,14,15,16,17]},
      {[<<"DB11136_DBW96">>,<<"DB11136_DBW98">>],[word,word]}
    ],

  {S7Addrs, Aliases} = build_addresses(L, As),
  ?assertEqual(Res, S7Addrs),
  ?assertEqual(AliasesList, Aliases).

-endif.