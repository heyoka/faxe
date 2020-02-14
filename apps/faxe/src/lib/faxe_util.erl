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
   , decimal_part/2, check_select_statement/1, clean_query/1, stringize_lambda/1]).

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
%% note also that trailing zeros will be truncated
-spec decimal_part(float(), non_neg_integer()) -> integer().
decimal_part(Float, Decimals) when is_float(Float), is_integer(Decimals) ->
   [_, Dec] =
      string:split(
         float_to_binary(Float, [compact, {decimals, Decimals}]),
         <<".">>
      ),
   binary_to_integer(Dec).

%% @doc
%% check if a given protocol prefix is present, if not prepend it
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

%% @doc clean an sql statement
-spec clean_query(binary()) -> binary().
clean_query(QueryBin) when is_binary(QueryBin) ->
   Q0 = re:replace(QueryBin, "\n|\t|\r|;", " ",[global, {return, binary}]),
   re:replace(Q0, "(\s){2,}", " ", [global, {return, binary}]).

%% check if the given string seems to be a valid "select ... from" statement
-spec check_select_statement(binary()|list()) -> true|false.
check_select_statement(Q) ->
   Query = clean_query(Q),
   Pattern = "^(S|s)(E|e)(L|l)(E|e)(C|c)(T|t)\s+(.*)\s+(F|f)(R|r)(O|o)(M|m)\s+(.*)",
   case re:run(Query, Pattern) of
      nomatch -> false;
      _ -> true
   end.

%% convert a fun() or a readable string

-spec stringize_lambda(function()) -> list().
stringize_lambda(Fun) when is_function(Fun) ->
   {env, [{_, _, _, Abs}]} = erlang:fun_info(Fun, env),
   Str = erl_pp:expr({'fun', 1, {clauses, Abs}}),
   io_lib:format("~s~p",[lists:flatten(Str)|"\n"]).

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
decimal_part_4_test() ->
   ?assertEqual(
      23,
      decimal_part(59.2300, 3)
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
check_select_1_test() ->
   ?assertEqual(
      true,
      check_select_statement(<<"SELECT * FROM table">>)
   ).
check_select_2_test() ->
   ?assertEqual(
      true,
      check_select_statement(<<"SELECT\n COUNT(*), AVG(field) FROM docs.table WHERE foo = 'bar'\n
      GROUP BY time ORDER BY time">>)
   ).
check_select_3_test() ->
   ?assertEqual(
      false,
      check_select_statement(<<"INSERT INTO table SET foo = 'bar'">>)
   ).
check_select_4_test() ->
   Sql = <<"SELECT floor(EXTRACT(epoch FROM time)/300)*300 AS time_gb, COUNT(*) FROM table
      WHERE tag1 = 'test' AND time >= $1 AND time <= $2 GROUP BY time_gb, a, b ORDER BY time_gb DESC">>,
   ?assertEqual(
      true,
      check_select_statement(Sql)
   ).
-endif.

