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
   , decimal_part/2, check_select_statement/1,
   clean_query/1, stringize_lambda/1,
   bytes_from_words/1, local_ip_v4/0,
   ip_to_bin/1, device_name/0, proplists_merge/2,
   levenshtein/2, build_topic/2, build_topic/1]).

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
   Pattern = "^\s?(S|s)(E|e)(L|l)(E|e)(C|c)(T|t)\s+(.*)\s+(F|f)(R|r)(O|o)(M|m)\s+(.*)",
   case re:run(Query, Pattern) of
      nomatch -> false;
      _ -> true
   end.

%% convert a fun() to a readable string
-spec stringize_lambda(function()) -> list().
stringize_lambda(Fun) when is_function(Fun) ->
   {env, [{_, _, _, Abs}]} = erlang:fun_info(Fun, env),
   Str = erl_pp:expr({'fun', 1, {clauses, Abs}}),
   io_lib:format("~s~p",[lists:flatten(Str)|"\n"]).

%% @doc convert words to bytes with respect to the system's wordsize
bytes_from_words(Words) ->
   try
      Words * erlang:system_info(wordsize)
   catch
      _:_ -> 0
   end.

-spec local_ip_v4() -> tuple().
local_ip_v4() ->
   {ok, Addrs} = inet:getifaddrs(),
   hd([
      Addr || {_, Opts} <- Addrs, {addr, Addr} <- Opts,
      size(Addr) == 4, Addr =/= {127,0,0,1}
   ]).

-spec ip_to_bin(tuple()) -> binary().
ip_to_bin({_A, _B, _C, _D} = Ip) ->
   list_to_binary(inet:ntoa(Ip)).

%% @doc try to get a unique name for the device we are running on
-spec device_name() -> binary().
device_name() ->
   %% first attempt is to try to get BALENA_DEVICE_UUID, should be set if we are running on balena
   case os:getenv(?KEY_BALENA_DEVICE_UUID) of
      false ->
         %% so we are not running on balena, then we use our local ip address
         %% @todo set env var for kubernetes
         Ip0 = ip_to_bin(local_ip_v4()),
         binary:replace(Ip0, <<".">>, <<"_">>, [global]);
      DeviceId ->
         list_to_binary(DeviceId)
   end.

proplists_merge(L, T) ->
   lists:ukeymerge(1, lists:keysort(1,L), lists:keysort(1,T)).


-spec build_topic(list(list()|binary())) -> binary().
build_topic(Parts) when is_list(Parts) ->
   build_topic(Parts, <<"/">>).

-spec build_topic(list(list()|binary()), binary()) -> binary().
build_topic(Parts, Separator) when is_list(Parts) andalso is_binary(Separator) ->
   PartsBin = [to_bin(Part) || Part <- Parts],
   PartsClean = [
      estr:str_replace_leading(
         estr:str_replace_trailing(P, Separator, <<>>
         ), Separator, <<>>)
      || P <- PartsBin, P /= Separator],
   iolist_to_binary(lists:join(Separator, PartsClean)).

to_bin(I) when is_list(I) -> list_to_binary(I);
to_bin(I) -> I.


%% Levenshtein code by Adam Lindberg, Fredrik Svensson via
%% http://www.trapexit.org/String_similar_to_(Levenshtein)
%%
%%------------------------------------------------------------------------------
%% @spec levenshtein(StringA :: string(), StringB :: string()) -> integer()
%% @doc Calculates the Levenshtein distance between two strings
%% @end
%%------------------------------------------------------------------------------
levenshtein(Samestring, Samestring) -> 0;
levenshtein(String, []) -> length(String);
levenshtein([], String) -> length(String);
levenshtein(Source, Target) ->
   levenshtein_rec(Source, Target, lists:seq(0, length(Target)), 1).

%% Recurses over every character in the source string and calculates a list of distances
levenshtein_rec([SrcHead|SrcTail], Target, DistList, Step) ->
   levenshtein_rec(SrcTail, Target, levenshtein_distlist(Target, DistList, SrcHead, [Step], Step), Step + 1);
levenshtein_rec([], _, DistList, _) ->
   lists:last(DistList).

%% Generates a distance list with distance values for every character in the target string
levenshtein_distlist([TargetHead|TargetTail], [DLH|DLT], SourceChar, NewDistList, LastDist) when length(DLT) > 0 ->
   Min = lists:min([LastDist + 1, hd(DLT) + 1, DLH + dif(TargetHead, SourceChar)]),
   levenshtein_distlist(TargetTail, DLT, SourceChar, NewDistList ++ [Min], Min);
levenshtein_distlist([], _, _, NewDistList, _) ->
   NewDistList.

% Calculates the difference between two characters or other values
dif(C, C) -> 0;
dif(_, _) -> 1.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% TESTS %%%%%%%%%
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
      check_select_statement(<<" SELECT\n COUNT(*), AVG(field) FROM docs.table WHERE foo = 'bar'\n
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

ip_to_bin_test() ->
   Ip = {127, 0, 0, 1},
   ?assertEqual(<<"127.0.0.1">>, ip_to_bin(Ip)).

build_topic_1_test() ->
   Res = faxe_util:build_topic([<<"/ttgw/sys/faxe/">>, <<"/conn_status/hee/">>, <<"flowid/nodeid/#/">>], <<"/">>),
   Expected = <<"ttgw/sys/faxe/conn_status/hee/flowid/nodeid/#">>,
   ?assertEqual(Expected, Res).
build_topic_2_test() ->
   Expected = <<"ttgw/sys/faxe/conn_status/hee/flowid/nodeid/#">>,
   Res = faxe_util:build_topic(["/ttgw/sys/faxe/", <<"conn_status/hee">>, <<"flowid/nodeid/#/">>, "/"], <<"/">>),
   ?assertEqual(Expected, Res).
build_topic_3_test() ->
   Expected = <<"ttgw.sys.faxe.conn_status.hee.flowid.nodeid.#">>,
   Res = faxe_util:build_topic([<<".ttgw.sys.faxe.">>, "conn_status.hee.", <<"flowid.nodeid.#">>], <<".">>),
   ?assertEqual(Expected, Res).

-endif.

