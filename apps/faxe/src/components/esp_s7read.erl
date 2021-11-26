%%%-------------------------------------------------------------------
%%% @author heyoka
%%% @copyright (C) 2019, <COMPANY>
%%% @doc
%%% get data from a siemens s7 plc via the snap7 library
%%% @end
%%% Created : 14. June 2019 11:32:22
%%%-------------------------------------------------------------------
-module(esp_s7read).
-author("heyoka").

%% API
-behavior(df_component).

-include("faxe.hrl").
%% API
-export([
  init/3, process/3, options/0,
  handle_info/2, shutdown/1,
  check_options/0
  , metrics/0]).

-export([
  maybe_emit/6, build_addresses/3,
  build_point/3, do_build/3]).

-define(MAX_READ_ITEMS, 19).
-define(DEFAULT_BYTE_LIMIT, 128).

-define(RECON_MIN_INTERVAL, 100).
-define(RECON_MAX_INTERVAL, 6000).
-define(RECON_MAX_RETRIES, infinity).

-record(state, {
  ip,
  port,
  slot,
  rack,
  client,
  interval,
  as :: list(binary()),
  as_prefix :: binary(),
  align,
  diff,
  timer_ref,
  vars, var_types :: list(binary()),
  vars_prefix :: binary(),
  last_values = [] :: list(),
  timer :: #faxe_timer{},
  opts,
  node_id,
  byte_size,
  connected = false,
  merge_field,
  port_data,
  address_offset
}).

options() -> [
  {ip, binary},
  {port, integer, 102},
  {every, duration, undefined},
  {align, is_set},
  {slot, integer, 1},
  {rack, integer, 0},
  {vars, string_list}, %% s7 addressing, ie: DB2024,Int16.1224 | DB2024.DBX12.2
  {vars_prefix, string, undefined},
  {as, binary_list},
  {as_prefix, string, undefined},
  {diff, is_set},
  {merge_field, string, undefined},
  {byte_offset, integer, 0},
  {use_pool, bool, {s7pool, enable}}
].

check_options() ->
  [
    {func, vars,
      fun(List, #{vars_prefix := VarsPrefix}) ->
        List1 = translate_vars(List, VarsPrefix),
        Invalid = lists:filter(fun(A) -> s7addr:parse(A) == {error, invalid} end, List1),
        case Invalid of
          [] -> true;
          _ -> {false, lists:join(<<", ">>, Invalid)}
        end
      end,
      <<", invalid address: ">>
    },
    {func, vars,
      fun(List, #{vars_prefix := VarsPrefix}) ->
        List1 = translate_vars(List, VarsPrefix),
        {P, _} = build_addresses(List1, lists:seq(1, length(List1)), 0),
        length(P) =< ?MAX_READ_ITEMS
      end,
      <<", has to many address items!">>
    },
    {func, vars,
      fun(List, #{vars_prefix := VarsPrefix}) ->
        List1 = translate_vars(List, VarsPrefix),
        {P, _} = build_addresses(List1, lists:seq(1, length(List1)), 0),
%%        lager:warning("bytes: ~p",[bit_count(P)/8]),
        bit_count(P)/8 =< ?DEFAULT_BYTE_LIMIT
      end,

      <<", byte-limit of 128 bytes exceeded!">>
    },

    {same_length, [vars, as]}
  ].

metrics() ->
  [
%%    {?METRIC_READING_TIME, histogram, [slide, 60]},
    {?METRIC_BYTES_READ, meter, []}
  ].

init({_, _NId}=NodeId, _Ins,
    #{ip := Ip,
      port := Port,
      every := Dur,
      align := Align,
      slot := Slot,
      rack := Rack,
      vars := Addresses,
      vars_prefix := Vars_Prefix,
      as := As,
      as_prefix := As_Prefix,
      diff := _Diff,
      merge_field := MergeField,
      byte_offset := Offset}=Opts) ->

%%  lager:notice("before: ~p ~n",[As]),
  As1 = translate_as(As, As_Prefix),
%%  lager:notice("after: ~p ~n",[As1]),
%%  lager:notice("before: ~p ~n",[Addresses]),
  Addresses1 = translate_vars(Addresses, Vars_Prefix),
%%  lager:notice("after: ~p ~n",[Addresses1]),
  {Parts, AliasesList} = build_addresses(Addresses1, As1, Offset),
  ByteSize = bit_count(Parts)/8,

  connection_registry:reg(NodeId, Ip, Port, <<"s7">>),
  S7Client = setup_connection(Opts),
  connection_registry:connecting(),

  %%  lager:info("~p VARS reduced to : ~p  with byte-size: ~p",[length(Addresses), length(Parts), bit_count(Parts)/8]),
  %%  [lager:notice("Partition: ~p", [Part]) || Part <- Parts],
  %%  [lager:notice("Aliases: ~p", [Part]) || Part <- AliasesList],

  {ok, all,
    #state{
      ip = Ip,
      port = Port,
      client = S7Client,
      as = AliasesList,
      slot = Slot,
      rack = Rack,
      align = Align,
      interval = Dur,
      diff = false, %Diff,
      vars = Parts,
      opts = Opts,
      node_id = NodeId,
      byte_size = ByteSize,
      merge_field = MergeField,
      address_offset = Offset
    }
  }.

setup_connection(Opts = #{use_pool := true}) ->
  s7pool_manager:connect(Opts),
  undefined;
setup_connection(Opts = #{use_pool := false}) ->
  {ok, Client} = s7worker:start_monitor(Opts),
  Client.


process(_Inport, _Item, State = #state{connected = false}) ->
  {ok, State};
process(_Inport, Item, State = #state{connected = true}) ->
  % read now trigger
  handle_info(poll, State#state{port_data = Item}).

handle_info({s7_connected, _Client}, State = #state{align = Align, interval = Dur}) ->
  lager:debug("s7_connected"),
  connection_registry:connected(),
  Timer = faxe_time:init_timer(Align, Dur, poll),
  {ok, State#state{timer = Timer, connected = true}};
handle_info({s7_disconnected, _Client}, State = #state{timer = Timer}) ->
  lager:debug("s7_disconnected"),
  connection_registry:disconnected(),
  NewTimer = faxe_time:timer_cancel(Timer),
  {ok, State#state{timer = NewTimer, connected = false}};
handle_info(poll, State = #state{connected = false}) ->
  {ok, State};
handle_info(poll, State=#state{as = Aliases, timer = Timer, byte_size = ByteSize,
      diff = Diff, last_values = LastList, node_id = FlowIdNodeId}) ->

  TStart = erlang:monotonic_time(microsecond),
  Result = read_vars(State),
  TMs = round((erlang:monotonic_time(microsecond)-TStart)/1000),
%%  case Timer /= undefined andalso TMs > Timer#faxe_timer.interval of
%%    true -> lager:notice("[~p] Time to read: ~p ms",[self(), TMs]);
%%    false -> ok
%%  end,
  case Result of
    {ok, Res} ->
      node_metrics:metric(?METRIC_READING_TIME, TMs, FlowIdNodeId),
      node_metrics:metric(?METRIC_ITEMS_IN, 1, FlowIdNodeId),
      node_metrics:metric(?METRIC_BYTES_READ, ByteSize, FlowIdNodeId),
      {Ts, NewTimer} =
        case is_record(Timer, faxe_timer) of
          true -> {Timer#faxe_timer.last_time, faxe_time:timer_next(Timer)};
          false -> {faxe_time:now(), Timer}
        end,
      NewState = State#state{timer = NewTimer, last_values = Res},
      maybe_emit(Diff, Ts, Res, Aliases, LastList, NewState);
    _Other ->
      node_metrics:metric(?METRIC_ERRORS, 1, FlowIdNodeId),
      lager:warning("Error reading S7 Vars: ~p", [_Other]),
      {ok, State#state{timer = faxe_time:timer_next(Timer)}}
  end;
handle_info({'DOWN', _Mon, process, Client, _Info}, State = #state{client = Client, opts = Opts, timer = Timer}) ->
  lager:notice("s7worker is down"),
  connection_registry:disconnected(),
  NewTimer = faxe_time:timer_cancel(Timer),
  NewClient = setup_connection(Opts),
  {ok, State#state{client = NewClient, timer = NewTimer, connected = false}};
handle_info(_E, S) ->
  lager:warning("got unexpected info: ~p",[_E]),
  {ok, S#state{}}.

shutdown(#state{timer = Timer, client = Client}) ->
  catch (gen_server:stop(Client)),
  catch (faxe_time:timer_cancel(Timer)).

read_vars(#state{client = undefined, vars = Vars, opts = ConnOpts}) ->
  s7pool_manager:read_vars(ConnOpts, Vars);
read_vars(#state{client = Client, vars = Vars}) ->
  s7worker:read(Client, Vars).

%%% @doc no diff flag -> emit
-spec maybe_emit(Diff :: true|false, Ts:: non_neg_integer(),
      ResultList :: list(), Aliases :: list(), LastResults :: list(), State :: #state{})
    -> ok | term().
maybe_emit(false, Ts, Res, Aliases, _, State = #state{merge_field = MField, port_data = PData}) ->
  Out0 = build_point(Ts, Res, Aliases),
  Out =
  case PData == undefined orelse MField == undefined of
    true -> Out0;
    _ ->
      flowdata:merge_points([PData, Out0], MField)
  end,
  {emit, {1, Out}, State#state{port_data = undefined}};
%%% @doc diff flag and result-list is exactly last list -> no emit
%% the power and the beauty of pattern matching ...
maybe_emit(true, _Ts, Result, _, Result, State) ->
  {ok, State};
%%% @doc diff flag -> emit values
maybe_emit(true, Ts, Result, Aliases, _Last, State) ->
  maybe_emit(false, Ts, Result, Aliases, [], State).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
translate_as(As, undefined) ->
  As;
translate_as(As, Prefix) ->
  [iolist_to_binary([Prefix, A]) || A <- As].

translate_vars(Vs, undefined) ->
  Vs;
translate_vars(Vs, Prefix) ->
  [iolist_to_binary([Prefix, V]) || V <- Vs].


build_addresses(Addresses, As, Offset) ->
  PList = [s7addr:parse(Address, Offset) || Address <- Addresses],
  %% inject Aliases into parameter maps

  AsAdds = lists:zip(As, PList),
%%  lager:info("after lists:zip" ,[AsAdds]),
  F = fun({Alias, Params}) -> Params#{as => Alias} end,
  WithAs = lists:map(F, AsAdds),
  %% partition addresses+aliases by data-type
  PartitionFun =
    fun(#{dtype := Dtype, start := Start} = E, Acc) ->
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
%%lager:notice("Splitted: ~p",[Splitted]),
  %% extract bit addresses
  {Bools, NonBools} =
  case maps:take(bool, Splitted) of
    error -> {[], Splitted};
    Other -> Other
  end,

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


build_point(Ts, ResultList, AliasesList) when is_list(ResultList), is_list(AliasesList) ->
  do_build(#data_point{ts=Ts}, ResultList, AliasesList).

do_build(Point=#data_point{}, [], []) ->
  Point;
do_build(Point=#data_point{}, [Res|R], [Aliases|AliasesList]) ->
  {Keys, Values} = bld(Res, Aliases),
  NewPoint = flowdata:set_fields(Point, Keys, Values),
  do_build(NewPoint, R, AliasesList).

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

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% TEST %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

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
    <<"DB11136.DBX102.1">>,
    <<"DB8034.DBS66.30">>],
  As = lists:map(fun(E) -> binary:replace(E, <<".">>, <<"_">>, [global]) end, L),
  Res = [
    #{amount => 1,area => db,db_number => 11136,start => 88,word_len => byte},
    #{amount => 5,area => db,db_number => 11136,start => 90,word_len => byte},
    #{amount => 3,area => db,db_number => 11136,start => 100,word_len => byte},
    #{amount => 2,area => db,db_number => 11136,start => 96,word_len => word},
    #{amount => 30,area => db,db_number => 8034,start => 66, word_len => byte}
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
      {[<<"DB11136_DBW96">>,<<"DB11136_DBW98">>],[word,word]},
      {[<<"DB8034_DBS66_30">>], [string]}
    ],

  {S7Addrs, Aliases} = build_addresses(L, As, 0),
  ?assertEqual(Res, S7Addrs),
  ?assertEqual(AliasesList, Aliases).

-endif.