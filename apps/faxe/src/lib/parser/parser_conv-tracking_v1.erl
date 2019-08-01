%%%-------------------------------------------------------------------
%%% @author heyoka
%%% @copyright (C) 2019, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 01. Aug 2019 16:53
%%%-------------------------------------------------------------------
-module('parser_conv-tracking_v1').
-author("heyoka").

-behavior(tcp_msg_parser).

%% API
-export([parse/1]).


-define(LINE_DELIMITER, <<";">>).
-define(FIELD_DELIMITER, <<",">>).
-define(VALUE_DELIMITER, <<":">>).
-define(SRC_PREFIX, <<"S">>).
-define(TARG_PREFIX, <<"T">>).

parse(BinData) ->
   [Timestamp|Lines] = to_lines(BinData),
   {Sources, Targets} = parse_lines(Lines),
   {Timestamp, Sources, Targets}.
%%,
%%   jsx:decode(BinData).

to_lines(BinData) ->
   binary:split(BinData, ?LINE_DELIMITER, [global, trim_all]).

parse_lines(Lines) when is_list(Lines) ->
   parse_lines(Lines, {[],[]}).
parse_lines([Line|Rest], Acc) ->

   parse_lines(Rest, Acc ++ line(to_fields(Line))).

to_fields(LineData) ->
   binary:split(LineData, ?FIELD_DELIMITER, [global, trim_all]).

line([<<?SRC_PREFIX, Add/binary>>, <<"CASE:", CaseNum/binary>>, <<"SRC1:", Src1Num/binary>>, REst]) ->
   ok;
line([<<?TARG_PREFIX, Add/binary>>, <<"CASE:", CaseNum/binary>>, <<"SRC1:", Src1Num/binary>>, REst]) ->
   ok.

