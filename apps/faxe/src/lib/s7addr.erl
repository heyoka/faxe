%%%-------------------------------------------------------------------
%%% @author heyoka
%%% @copyright (C) 2019
%%% @doc
%%% siemens s7 addressing
%%% (simatic (german) and IEC (english) mnemonics are supported)
%%% @see https://www.npmjs.com/package/node-red-contrib-s7 for addressing
%%% no peripheral addresses supported, no string addresses supported!
%%%
%%% @todo handle string type
%%% @end
%%% Created : 15. Jun 2019 20:05
%%%-------------------------------------------------------------------
-module(s7addr).
-author("heyoka").

%% API
-export([parse/1]).

-spec parse(binary()) -> map().
parse(Address) when is_binary(Address) ->
   Addr = string:uppercase(Address),
   %% trim whitespace
   Clean = binary:replace(Addr, <<" ">>, <<>>, [global]),
   %% check for addressing format // "DB" starts at index 0
   case string:find(Clean, <<"DB">>) of
      Clean ->
            Pattern =
            case string:find(Clean, <<",">>) of
               nomatch  -> <<".">>;
               _        -> <<",">>
            end,
            do_parse(binary:split(Clean, Pattern, [trim_all]));

      _ ->  do_parse(Clean)
   end.

%% non db address
do_parse([<<First/binary>>]) ->
   Parts = binary:split(First, <<".">>, [trim_all]),
   parse_non_db(Parts, #{amount => 1});
%% db address step7 style
do_parse([<<"DB", _DbNumber/binary>> = First, <<"DB", Part2/binary>>]) ->
   do_parse([First, Part2]);
%% db address nodered style
do_parse([<<"DB", DbNumber/binary>>, Part2]) ->
   P = #{area => db, amount => 1, db_number => binary_to_integer(DbNumber)},
   Parts = binary:split(Part2, <<".">>, [trim_all]),
   parse_db(Parts, P);


do_parse(_) -> invalid.

%% second part after comma !
parse_db([NoBitAccess], Params) ->
   DataType = clear_numbers(NoBitAccess),
   Size = byte_size(DataType),
   <<DataType:Size/binary, StartAddr/binary>> = NoBitAccess,
   {DType, CDType} = data_type(DataType),
   Params#{word_len => DType, start => binary_to_integer(StartAddr), dtype => CDType};
parse_db([BitAccess, Bit], Params) ->
   DataType = clear_numbers(BitAccess),
   Size = byte_size(DataType),
   <<DataType:Size/binary, StartAddr/binary>> = BitAccess,
   {DType,CDType} = data_type(DataType),
   {Start, Amount} = start_amount(DType, StartAddr, Bit),
   Params#{start => Start, amount => Amount, word_len => DType, dtype => CDType}.


%% @doc
%% non db
%%
parse_non_db([NoBitAccess], Par) ->
   % clear numbers to get the area
   Area = clear_numbers(NoBitAccess),
   Size = byte_size(Area),
   <<Area:Size/binary, StartAddr/binary>> = NoBitAccess,
   P = type(Area, Par#{start => binary_to_integer(StartAddr)}),
   {P, NoBitAccess};
parse_non_db([WithBitAccess, Bit], Par) ->
   Area = clear_numbers(WithBitAccess),
   Size = byte_size(Area),
   <<Area:Size/binary, StartAddr/binary>> = WithBitAccess,
   P = type(Area, Par#{}),
   {Start, Amount} = start_amount(maps:get(word_len, P), StartAddr, Bit),
   {P#{start => Start, amount => Amount}, WithBitAccess, Bit}.

clear_numbers(Bin) ->
   re:replace(Bin, "[0-9]", <<>>, [{return, binary}, global]).

%%%% standard inputs

-spec type(binary(), map()) -> map().
type(<<"I">>, P) ->
   type(<<"E">>, P);
type(<<"E">>, P) ->
   P#{area => pe, word_len => bit, dtype => bool};
type(<<"IB">>, P) ->
   type(<<"EB">>, P);
type(<<"EB">>, P) ->
   P#{area => pe, word_len => byte, dtype => byte};
type(<<"IC">>, P) ->
   type(<<"EC">>, P);
type(<<"EC">>, P) ->
   P#{area => pe, word_len => byte, dtype => char}; %% char
type(<<"IW">>, P) ->
   type(<<"EW">>, P);
type(<<"EW">>, P) ->
   P#{area => pe, word_len => word, dtype => word};
type(<<"II">>, P) ->
   type(<<"EI">>, P);
type(<<"EI">>, P) ->
   P#{area => pe, word_len => byte, dtype => int}; %% integer

type(<<"ID">>, P) ->
   type(<<"ED">>, P);
type(<<"ED">>, P) ->
   P#{area => pe, word_len => d_word, dtype => d_word};
type(<<"IDI">>, P) ->
   type(<<"EDI">>, P);
type(<<"EDI">>, P) ->
   P#{area => pe, word_len => d_word, dtype => d_int}; %% DINT ?

type(<<"IR">>, P) ->
   type(<<"ER">>, P);
type(<<"ER">>, P) ->
   P#{area => pe, word_len => real, dtype => float};

%%% standard outputs
type(<<"Q">>, P) ->
   type(<<"A">>, P);
type(<<"A">>, P) ->
   P#{area => pa, word_len => bit, dtype => bool};
type(<<"QB">>, P) ->
   type(<<"AB">>, P);
type(<<"AB">>, P) ->
   P#{area => pa, word_len => byte, dtype => byte};
type(<<"QC">>, P) ->
   type(<<"AC">>, P);
type(<<"AC">>, P) ->
   P#{area => pa, word_len => byte, dtype => char}; %% char

type(<<"QW">>, P) ->
   type(<<"AW">>, P);
type(<<"AW">>, P) ->
   P#{area => pa, word_len => word, dtype => word};
type(<<"QI">>, P) ->
   type(<<"AI">>, P);
type(<<"AI">>, P) ->
   P#{area => pa, word_len => byte, dtype => int}; %% integer

type(<<"QD">>, P) ->
   type(<<"AD">>, P);
type(<<"AD">>, P) ->
   P#{area => pa, word_len => d_word, dtype => dword};
type(<<"QDI">>, P) ->
   type(<<"ADI">>, P);
type(<<"ADI">>, P) ->
   P#{area => pa, word_len => d_word, dtype => d_int}; %% DINT ?

type(<<"QR">>, P) ->
   type(<<"AR">>, P);
type(<<"AR">>, P) ->
   P#{area => pa, word_len => real, dtype => float};

%%% markers
type(<<"M">>, P) ->
   P#{area => mk, word_len => bit, dtype => bool};
type(<<"MB">>, P) ->
   P#{area => mk, word_len => byte, dtype => byte};
type(<<"MC">>, P) ->
   P#{area => mk, word_len => byte, dtype => char}; %% char
type(<<"MW">>, P) ->
   P#{area => mk, word_len => word, dtype => word};
type(<<"MI">>, P) ->
   P#{area => mk, word_len => byte, dtype => int}; %% integer
type(<<"MD">>, P) ->
   P#{area => mk, word_len => d_word, dtype => d_int};
type(<<"MDI">>, P) ->
   P#{area => mk, word_len => d_word, dtype => d_int}; %% DINT ?
type(<<"MR">>, P) ->
   P#{area => mk, word_len => real, dtype => float};

%%% others
type(<<"T">>, P) ->
   P#{area => tm, word_len => timer, dtype => timer};
type(<<"C">>, P) ->
   P#{area => ct, word_len => counter, dtype => counter}.


%% dtype for db
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec data_type(binary()) -> {Snap7WordType :: atom(), DesiredType :: atom()}.
data_type(<<"I">>) -> data_type(<<"INT">>);
data_type(<<"INT">>) -> {word, int};

data_type(<<"D">>) -> data_type(<<"DINT">>);
data_type(<<"DI">>) -> data_type(<<"DINT">>);
data_type(<<"DINT">>) -> {d_word, d_int};

data_type(<<"DR">>) -> data_type(<<"REAL">>);
data_type(<<"R">>) -> data_type(<<"REAL">>);
data_type(<<"REAL">>) -> {real, float};

data_type(<<"X">>) -> {bit, bool};

data_type(<<"B">>) -> data_type(<<"BYTE">>);
data_type(<<"BYTE">>) -> {byte, byte};

data_type(<<"C">>) -> data_type(<<"CHAR">>);
data_type(<<"CHAR">>) -> {byte, char};

data_type(<<"W">>) -> data_type(<<"WORD">>);
data_type(<<"WORD">>) -> {word, word};

data_type(<<"DW">>) -> data_type(<<"DWORD">>);
data_type(<<"DWORD">>) -> {d_word, d_word};

data_type(<<"S">>) -> data_type(<<"STRING">>);
data_type(<<"STRING">>) -> {byte, string}.


%%%%%
start_amount(WordType, StartMarker, BitMarker) ->
   case WordType of
      bit   -> {binary_to_integer(StartMarker)*8 + binary_to_integer(BitMarker), 1};
      _     -> {binary_to_integer(StartMarker), binary_to_integer(BitMarker)}
   end.