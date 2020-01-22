%%%-------------------------------------------------------------------
%%% @author heyoka
%%% @copyright (C) 2019, <COMPANY>
%%% @doc
%%% some utility functions
%%% @end
%%% Created : 09. Dec 2019 09:57
%%%-------------------------------------------------------------------
-module(faxe_util).
-author("heyoka").

-include("faxe.hrl").

%% API
-export([
   uuid_string/0, round_float/2,
   prefix_binary/2,
   host_with_protocol/1, host_with_protocol/2
   , decimal_part/2]).

-define(HTTP_PROTOCOL, <<"http://">>).

%% @doc get a uuid v4 binary string
-spec uuid_string() -> binary().
uuid_string() ->
   uuid:uuid_to_string(uuid:get_v4(strong)).

%% @doc round a floating point number with a given precision
-spec round_float(float(), non_neg_integer()) -> float().
round_float(Float, Precision) when is_float(Float), is_integer(Precision) ->
   list_to_float(io_lib:format("~.*f",[Precision, Float])).

%% @doc get the decimal part of a float as an integer
%% note that rounding occurs if there are more decimals in the float than given by the parameter
-spec decimal_part(float(), non_neg_integer()) -> integer().
decimal_part(Float, Decimals) when is_float(Float), is_integer(Decimals) ->
   [_, Dec] = string:split(float_to_binary(Float, [compact, {decimals, Decimals}]), <<".">>),
   binary_to_integer(Dec).

%% @doc
%% check if a given protocol prefix is present, if not prepend it
%%
-spec host_with_protocol(binary()) -> binary().
host_with_protocol(Host) when is_binary(Host) ->
   prefix_binary(Host, ?HTTP_PROTOCOL).
host_with_protocol(Host, Protocol) when is_binary(Host) ->
   prefix_binary(Host, Protocol).

%% @doc check if a given binary string is a prefix, if not prepend it
-spec prefix_binary(binary(), binary()) -> binary().
prefix_binary(Bin, Prefix) when is_binary(Bin), is_binary(Prefix) ->
      case string:find(Bin, Prefix) of
         nomatch -> <<Prefix/binary, Bin/binary>>;
         _ -> Bin
      end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% TESTS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-ifdef(TEST).
decimal_part_test() ->
   ?assertEqual(
     232,
      decimal_part(59.232, 3)
   ).
decimal_part_2_test() ->
   ?assertEqual(
      233,
      decimal_part(59.23266, 3)
   ).
decimal_part_3_test() ->
   ?assertEqual(
      23,
      decimal_part(59.23, 3)
   ).
round_float_test() ->
   ?assertEqual(
      1234.232,
      round_float(1234.23214, 3)
   ).
round_float_2_test() ->
   ?assertEqual(
      2342.4567,
      round_float(2342.4567, 4)
   ).
round_float_3_test() ->
   ?assertEqual(
      3.456,
      round_float(3.456, 5)
   ).
prefix_binary_test() ->
   ?assertEqual(
     <<"myprefix">>,
      prefix_binary(<<"prefix">>, <<"my">>)
   ).

prefix_binary_2_test() ->
   ?assertEqual(
      <<"myprefix">>,
      prefix_binary(<<"myprefix">>, <<"m">>)
   ).
-endif.

