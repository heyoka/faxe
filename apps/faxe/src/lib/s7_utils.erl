%%%-------------------------------------------------------------------
%%% @author heyoka
%%% @copyright (C) 2022, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 22. Jun 2022 19:24
%%%-------------------------------------------------------------------
-module(s7_utils).
-author("heyoka").

%% API

-export([add_client_addresses/3, build_addresses/2, remove_client/3, merge_addresses/2, bit_count/1]).

-define(S7_HEADER_LENGTH_BYTES, 7).
-define(S7_FUNCTION_HEADER_BYTES, 12).
-define(S7_HEADER_BYTES, ?S7_HEADER_LENGTH_BYTES+?S7_FUNCTION_HEADER_BYTES).
-define(S7_VAR_OVERHEAD_BYTES, 12).
-define(S7_MAX_REQUEST_VARS, 19).


add_client_addresses(Client, Addresses, CurrentAddresses) ->
  NewAddresses =
    lists:foldl(
      fun({Field, Add}, TheAddresses1) ->
        ClientIdx = {Client, Field},
        case lists:keytake(Add, 1, TheAddresses1) of
          {value, {_Add, Clients}, TheAddresses2} ->
            lists:keystore(Add, 1, TheAddresses2, {Add, [ClientIdx | Clients]});
          false ->
            [{Add, [ClientIdx]} | TheAddresses1]
        end
      end,
      CurrentAddresses,
      Addresses),
  NewAddresses.

merge_addresses(StartAddresses, AddressList) when is_list(StartAddresses), is_list(AddressList) ->
    lists:foldl(
      fun({Address, Clients}, CurrentAddresses) ->
        case lists:keytake(Address, 1, CurrentAddresses) of
          {value, {Address, CurrentClients}, CurrentAddresses2} ->
            lists:keystore(Address, 1, CurrentAddresses2, {Address, Clients++CurrentClients});
          false ->
            [{Address, Clients} | CurrentAddresses]
        end
      end,
      StartAddresses,
      AddressList
    ).

remove_client(Client, Addresses, CurrentAddresses) ->
  lists:foldl(
    fun({_Field, Add}, Current) ->
      case lists:keytake(Add, 1, Current) of
        {value, {_Add, [{Client, _}]}, Current2} ->
          %% if this client is the only one for the current address, then remove the whole address
           Current2;
        {value, {_Add, Clients}, Current2} ->
          %% otherwise delete just the client from the list
          lists:keystore(Add, 1, Current2, {Add, proplists:delete(Client, Clients)});
          %% should not be the case
        false ->
          Current
      end
    end,
    CurrentAddresses,
    Addresses).


build_addresses(Addresses, PDUSize) ->
  lager:notice("building addresses with PDU size ~p",[PDUSize]),
  F = fun({Address, Clients}) -> Address#{clients => Clients} end,
  WithClients = lists:map(F, Addresses),
%%  {WithClients, _} = lists:unzip(WithClients0),

%%  lager:notice("with clients: ~p",[WithClients]),

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
  Splitted = lists:foldl(PartitionFun, #{}, WithClients),
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
%%  {BoolParts, BoolClients} = find_bool_bytes(BoolsSorted),
%%  lager:notice("BOOL CLients: ~p",[BoolClients]),
  {NewBoolParts, {NewBoolRestParts, _NewBoolRestAliases} = BoolRest} = find_bool_bytes(BoolsSorted, PDUSize),
%%  lager:warning("NEW bool contiguous: ~n",[]),
%%  lager:warning("NEW BOOL Parts: ~p~n",[NewBoolParts]),
%%  lager:warning("NEW BOOL RESTParts: ~p~n",[length(NewBoolRestParts)]),
%%  lager:warning("NEW BOOL RESTAliases: ~p~n",[NewBoolRestAliases]),

  %% sort by starts
  ParamsSorted = lists:flatmap(fun({_Type, L}) -> sort_by_start(L) end, maps:to_list(NonBools)),
  %% find contiguous starts
%%  {NonBoolParts, NonBoolAliases} = find_contiguous(ParamsSorted),
  {NewParts, {NewRestParts, NewRestAliases} = NonBoolRest} = find_contiguous(ParamsSorted, PDUSize),
%%  lager:warning("NEW contiguous: ~n",[]),
%%  lager:warning("NEW Parts: ~p~n",[NewParts]),
%%  lager:warning("NEW RESTParts: ~p~n",[length(NewRestParts)]),
%%  lager:warning("NEW RESTAliases: ~p~n",[NewRestAliases]),

  %% merge rest from bool and non bools, if size would allow it
  AllRestParts = NewBoolRestParts++NewRestParts,
  All =
  case exceeds_limits(PDUSize, AllRestParts) of
    true ->
      lager:warning("cannot merge rest parts"),
      NewBoolParts ++ [BoolRest] ++ NewParts ++ [NonBoolRest];
    false ->
      lager:warning("merge rest parts"),
      Rest0 = [BoolRest, NonBoolRest],
%%      lager:notice("Rests now ~p",[Rest0]),
      Rest = lists:filter(
        fun
          ({[], []}) -> false;
          (_) -> true
        end,
        Rest0),
%%      lager:notice("Rests after filter ~p",[Rest0]),
      A = NewBoolParts ++ NewParts ++ Rest,
%%      [lager:notice("All merged REST ~p",[A1]) || A1 <- A],
      A
end,
  [lager:notice("request num items: ~p - ~p byte",[length(Req), byte_count(Req)]) || {Req, _} <- All],
  All.


find_contiguous([], _PDUSize) -> {[], {[], []}};
find_contiguous(ParamList, PDUSize) ->
  F = fun(
      #{start := Start, clients := Clients, db_number := DB, dtype := DType} = E,
      {LastStart,
        Current = #{aliases := CAs, amount := CAmount, db_number := CDB, dtype := CType},
        CurrentVars,
        Requests,
        CurrentSize}
  ) ->
    ThisSize = byte_count(E),
    NewSize = ThisSize + CurrentSize,
%%    lager:info("CurrentSize is ~p",[CurrentSize]),
%%    lager:info("CurrentVars is ~p",[CurrentVars]),
%%    lager:info("Current is ~p",[Current]),
    CMaxPayloadSize = max_payload_size(PDUSize, length(CurrentVars)),
    case size_exceeded(PDUSize, NewSize, length(CurrentVars)) of
%%    case exceeds_limits(PDUSize, NewSize, length(CurrentVars)) of
      true ->
%%        lager:notice("new size of ~p > ~p at:~p",[NewSize, CMaxPayloadSize, E]),
%%        %% we need a new request here
%%        NewCurrent = Current#{amount => CAmount+1, aliases => CAs++[{Clients, DType}]},
        NewCurrentVars = CurrentVars++[Current],
%%        lager:info("RealByteSize: ~p",[byte_count(NewCurrentVars)]),
        AliasesList = lists:map(fun(#{aliases := Aliases}) -> lists:unzip(Aliases) end, NewCurrentVars),
        AddressPartitions = [maps:without([aliases, clients, dtype], M) || M <- NewCurrentVars],
        {-2, E#{aliases => [{Clients, DType}]}, [], Requests ++[{AddressPartitions, AliasesList}], ThisSize};
      false ->
        case (DType == CType) andalso (DB == CDB) andalso (LastStart + word_len_size(DType) == Start) of
          true ->
            NewCurrent = Current#{amount => CAmount+1, aliases => CAs++[{Clients, DType}]},
            {Start, NewCurrent, CurrentVars, Requests, NewSize};
          false ->
            RealCurrentVars =
            case Current of
              #{amount := 0, db_number := -1} -> CurrentVars;
              _ -> CurrentVars++[Current]
            end,
            case item_count_exceeded(length(RealCurrentVars)) of
              true ->
%%                lager:notice("item count exceeded ~p at ~p, CURRENT is: ~p", [length(RealCurrentVars), E, Current]),
                AliasesList = lists:map(fun(#{aliases := Aliases}) -> lists:unzip(Aliases) end, CurrentVars),
                AddressPartitions = [maps:without([aliases, clients, dtype], M) || M <- CurrentVars],
                {-2, E#{aliases => [{Clients, DType}]}, [Current], Requests ++[{AddressPartitions, AliasesList}], ThisSize};
              false ->
                {Start, E#{aliases => [{Clients, DType}]}, RealCurrentVars, Requests, NewSize}
            end
        end
    end
      end,
  {_Last, Current, CurrentVars, Parts, _Size} =
    lists:foldl(F, {-2, #{aliases => [], amount => 0, db_number => -1, dtype => nil}, [], [], 0}, ParamList),

%%    lager:notice("NONBOOL Current ~p~nCurrentVars ~p~nParts ~p",[Current, CurrentVars, Parts]),
  RestVars = [Current|CurrentVars],
%%  lager:notice("RestVars ~p: ~p",[length(RestVars), RestVars]),
%%  [lager:notice("~nnonbool Part: ~p",[Part]) || Part <- Parts],

  FAs = fun(#{aliases := Aliases}) -> lists:unzip(Aliases) end,
  RestAliasesList = lists:map(FAs, RestVars),
  RestAddressPartitions = [maps:without([aliases, clients, dtype], M) || M <- RestVars],
%%  lager:notice("REST Addresses ~p",[RestAddressPartitions]),
  %% we have to filter Parts for empty address entries
%%  Parts = lists:filter(fun({Var, _Als}) ->
  {Parts, {RestAddressPartitions, RestAliasesList}}.


find_bool_bytes([], _PDUSize) -> {[], {[], []}};
find_bool_bytes(Bools, PDUSize) ->
  CFun = fun(
      #{byte_num := Byte, db_number := DB, clients := Clients, bit_num := Bit} = E,
      {LastByte,
        Current = #{aliases := CAs, db_number := CDB, amount := CAmount, start := CStartByte},
        CurrentVars,
        Requests,
        CurrentSize}
  ) ->
%%    lager:info("Clients in find_bool_bytes: ~p",[Clients]),
    ThisSize = byte_count(E),
    NewSize = ThisSize + CurrentSize,
%%    lager:info("B CurrentSize is ~p",[CurrentSize]),
%%    lager:info("B CurrentVars is ~p ~p",[CurrentVars, length(CurrentVars)]),
%%    lager:info("B Current is ~p",[Current]),
    case size_exceeded(PDUSize, NewSize, length(CurrentVars)) of
%%    case exceeds_limits(PDUSize, NewSize, length(CurrentVars)) of
      true ->
        CMaxPayloadSize = max_payload_size(PDUSize, length(CurrentVars)),
%%        lager:notice("B new size of ~p > ~p at : ~p",[NewSize, CMaxPayloadSize, E]),
        %% we need a new request here
%%        NewCurrent0 = Current#{aliases => CAs++[{Clients, bool_byte, (Bit+(Byte-CStartByte)*8)}]},
%%        NewCurrent =
%%          case LastByte + 1 == Byte of
%%            true -> NewCurrent0#{amount => CAmount+1};
%%            false -> NewCurrent0
%%          end,
        NewCurrentVars = CurrentVars++[Current],
%%        lager:info("B RealByteSize: ~p",[byte_count(NewCurrentVars)]),
        AliasesList = lists:map(fun(#{aliases := Aliases}) -> lists:unzip3(Aliases) end, NewCurrentVars),
        AddressPartitions = [maps:without([aliases, clients, dtype, byte_num, bit_num], M) || M <- NewCurrentVars],
        %% reset current byte with -2
        {-2, E#{aliases => [{Clients, bool_byte, Bit}]}, [], Requests ++[{AddressPartitions, AliasesList}], ThisSize};
      false ->
        case (DB == CDB) andalso (LastByte == Byte orelse (Byte == LastByte + 1 andalso Bit == 0)) of
          true ->
            NewCurrent0 = Current#{aliases => CAs++[{Clients, bool_byte, (Bit+(Byte-CStartByte)*8)}]},
            NewCurrent =
              case LastByte + 1 == Byte of
                true -> NewCurrent0#{amount => CAmount+1};
                false -> NewCurrent0
              end,
            {Byte, NewCurrent, CurrentVars, Requests, NewSize};
          false ->
            RealCurrentVars =
            case Current of
              #{amount := 0, db_number := -1} -> CurrentVars;
              _ -> CurrentVars++[Current]
            end,
            case item_count_exceeded(length(RealCurrentVars)) of
              true ->
%%                lager:notice("item count exceeded ~p at ~p, CURRENT is: ~p", [length(RealCurrentVars), E, Current]),
                AliasesList = lists:map(fun(#{aliases := Aliases}) -> lists:unzip3(Aliases) end, CurrentVars),
                AddressPartitions = [maps:without([aliases, clients, dtype, byte_num, bit_num], M) || M <- CurrentVars],
                %% reset current byte with -2
                {-2, E#{aliases => [{Clients, bool_byte, Bit}]}, [Current], Requests ++[{AddressPartitions, AliasesList}], ThisSize};
              false ->
                {Byte,
                  E#{amount => 1, start => Byte, word_len => byte,
                    aliases => [{Clients, bool_byte, Bit}]}, RealCurrentVars, Requests, NewSize}
            end

        end
    end
         end,
  {_Last, Current, CurrentVars, Parts, _Size} =
    lists:foldl(CFun, {-2, #{aliases => [], amount => 0, db_number => -1, start => -2}, [], [], 0}, Bools),
%%  lager:alert("BOOL Current ~p~nCurrentVars ~p~nParts ~p",[Current, CurrentVars, Parts]),

  RestVars = CurrentVars ++ [Current],
%%  All = Parts ++ [Current],
%%  lager:notice("ALL bools: ~p",[All]),
  FAs = fun(#{aliases := Aliases}) -> lists:unzip3(Aliases) end,
  RestAliasesList = lists:map(FAs, RestVars),
  RestAddressPartitions = [maps:without([aliases, clients, dtype, byte_num, bit_num], M) || M <- RestVars],
  {Parts, {RestAddressPartitions, RestAliasesList}}.


%% sort a list of parsed s7 address maps by (db_number+)start
sort_by_start(ParamList) ->
  lists:sort(
    fun(#{start := StartA, db_number := DbA}, #{start := StartB, db_number := DbB}) ->
      %% we multiple the db_number by 10000 to avoid getting db-start addresses mixed up accidentally
      (DbA*10000 + StartA) < (DbB*10000 + StartB) end,
    ParamList).

exceeds_limits(PDUSize, VarList) when is_list(VarList) ->
  exceeds_limits(PDUSize, byte_count(VarList), length(VarList)).

exceeds_limits(PDUSize, PayloadSize, VarListCount) ->
  item_count_exceeded(VarListCount) orelse size_exceeded(PDUSize, PayloadSize, VarListCount).

item_count_exceeded(VarListCount) ->
  VarListCount > ?S7_MAX_REQUEST_VARS.

size_exceeded(PDUSize, PayloadSize, VarListCount) ->
  PayloadSize >= max_payload_size(PDUSize, VarListCount).

max_payload_size(PDUSize, NumItems) ->
  PDUSize - (?S7_HEADER_LENGTH_BYTES + ?S7_FUNCTION_HEADER_BYTES + (NumItems * ?S7_VAR_OVERHEAD_BYTES)).

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


byte_count(VarList) when is_list(VarList) ->
  bit_count(VarList) / 8;
byte_count(Var) ->
  byte_count([Var]).

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
