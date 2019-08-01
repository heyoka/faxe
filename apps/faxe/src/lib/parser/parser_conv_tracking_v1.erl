%%%-------------------------------------------------------------------
%%% @author heyoka
%%% @copyright (C) 2019, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 01. Aug 2019 16:53
%%%-------------------------------------------------------------------
-module(parser_conv_tracking_v1).
-author("heyoka").

-behavior(tcp_msg_parser).

%% API
-export([parse/1, test/0]).


-define(LINE_DELIMITER, <<";">>).
-define(FIELD_DELIMITER, <<",">>).
-define(VALUE_DELIMITER, <<":">>).
-define(SRC_PREFIX, "S").
-define(TARG_PREFIX, "T").

parse(BinData) ->
   LinesAll = to_lines(BinData),
   lager:notice("Lines: ~p",[LinesAll]),
   [Timestamp|Lines] = LinesAll,
   {Sources, Targets} = parse_lines(Lines),
   lager:info("lineCount: ~p~n",[length(Lines)]),
   {Timestamp, Sources, Targets}.
%%,
%%   jsx:decode(BinData).

to_lines(BinData) ->
   binary:split(BinData, ?LINE_DELIMITER, [global, trim_all]).

%%p_parse_lines(Lines) when is_list(Lines) ->
%%   lists:foldl(fun(Line, Acc) -> line(to_fields(Line), Acc) end, {[],[]}, Lines).
%%   plists_new:fold(fun(Line, Acc) -> line(to_fields(Line), Acc) end, {[],[]}, Lines, {processes, 3}).

parse_lines(Lines) when is_list(Lines) ->
   parse_lines(Lines, {[],[]}).

parse_lines([], {Sources, Targets}) ->
   {lists:reverse(Sources), lists:reverse(Targets)};
parse_lines([Line|Rest], Acc) ->
   NewAcc = line(to_fields(Line), Acc),
   parse_lines(Rest, NewAcc).

to_fields(LineData) ->
   binary:split(LineData, ?FIELD_DELIMITER, [global, trim_all]).

%% line with source definition
line([
   << ?SRC_PREFIX, Add/binary>>,
   <<"CASE:", CaseNum/binary>>,
   <<"SRC1:", Src1Num/binary>>,
   REst], {Srcs, Tgts}) ->
   {[#{source => Add, case1 => CaseNum, src1 => Src1Num, rest => REst}|Srcs], Tgts};

%% line with target definition
line([
   << ?TARG_PREFIX, Add/binary>>,
   <<"CASE:", CaseNum/binary>>,
   <<"SRC1:", Src1Num/binary>>,
   REst], {Srcs, Tgts}) ->
   {Srcs, [#{target => Add, case1 => CaseNum, src1 => Src1Num, rest => REst}|Tgts]}.


%% test
def() ->
   erlang:iolist_to_binary([
   <<"18:08:2018T22:07:24:345Z;S10,CASE:33,SRC1:432,whatever;T43,CASE:17,SRC1:66,whatever2;">>,
      <<"S10,CASE:33,SRC1:432,some_source;T0043,CASE:17,SRC1:634,whatever2;">>,
      <<"S10,CASE:33,SRC1:432,some_source;T0043,CASE:17,SRC1:634,whatever2;">>,
      <<"S10,CASE:33,SRC1:432,some_source;T0043,CASE:17,SRC1:634,whatever2;">>,
      <<"S10,CASE:33,SRC1:432,some_source;T0043,CASE:17,SRC1:634,whatever2;">>,
      <<"S10,CASE:33,SRC1:432,some_source;T0043,CASE:17,SRC1:634,whatever2;">>,
      <<"S10,CASE:33,SRC1:432,some_source;T0043,CASE:17,SRC1:634,whatever2;">>,
      <<"S10,CASE:33,SRC1:432,some_source;T0043,CASE:17,SRC1:634,whatever2;">>,
      <<"S10,CASE:33,SRC1:432,some_source;T0043,CASE:17,SRC1:634,whatever2;">>,
      <<"S10,CASE:33,SRC1:432,some_source;T0043,CASE:17,SRC1:634,whatever2;">>,
      <<"S10,CASE:33,SRC1:432,some_source;T0043,CASE:17,SRC1:634,whatever2;">>,
      <<"S10,CASE:33,SRC1:432,some_source;T0043,CASE:17,SRC1:634,whatever2;">>,
      <<"S10,CASE:33,SRC1:432,some_source;T0043,CASE:17,SRC1:634,whatever2;">>,
      <<"S10,CASE:33,SRC1:432,some_source;T0043,CASE:17,SRC1:634,whatever2;">>,
      <<"S10,CASE:33,SRC1:432,some_source;T0043,CASE:17,SRC1:634,whatever2;">>,
      <<"S10,CASE:33,SRC1:432,some_source;T0043,CASE:17,SRC1:634,whatever2;">>,
      <<"S10,CASE:33,SRC1:432,some_source;T0043,CASE:17,SRC1:634,whatever2;">>,
      <<"S10,CASE:33,SRC1:432,some_source;T0043,CASE:17,SRC1:634,whatever2;">>,
      <<"S10,CASE:33,SRC1:432,some_source;T0043,CASE:17,SRC1:634,whatever2;">>,
      <<"S10,CASE:33,SRC1:432,some_source;T0043,CASE:17,SRC1:634,whatever2;">>,
      <<"S10,CASE:33,SRC1:432,some_source;T0043,CASE:17,SRC1:634,whatever2;">>,
      <<"S10,CASE:33,SRC1:432,some_source;T0043,CASE:17,SRC1:634,whatever2;">>,
      <<"S10,CASE:33,SRC1:432,some_source;T0043,CASE:17,SRC1:634,whatever2;">>,
      <<"S10,CASE:33,SRC1:432,some_source;T0043,CASE:17,SRC1:634,whatever2;">>,
      <<"S10,CASE:33,SRC1:432,some_source;T0043,CASE:17,SRC1:634,whatever2;">>,
      <<"S10,CASE:33,SRC1:432,some_source;T0043,CASE:17,SRC1:634,whatever2;">>,
      <<"S10,CASE:33,SRC1:432,some_source;T0043,CASE:17,SRC1:634,whatever2;">>,
      <<"S10,CASE:33,SRC1:432,some_source;T0043,CASE:17,SRC1:634,whatever2;">>,
      <<"S10,CASE:33,SRC1:432,some_source;T0043,CASE:17,SRC1:634,whatever2;">>,
      <<"S10,CASE:33,SRC1:432,some_source;T0043,CASE:17,SRC1:634,whatever2;">>,
      <<"S10,CASE:33,SRC1:432,some_source;T0043,CASE:17,SRC1:634,whatever2;">>,
      <<"S10,CASE:33,SRC1:432,some_source;T0043,CASE:17,SRC1:634,whatever2;">>,
      <<"S10,CASE:33,SRC1:432,some_source;T0043,CASE:17,SRC1:634,whatever2;">>,
      <<"S10,CASE:33,SRC1:432,some_source;T0043,CASE:17,SRC1:634,whatever2;">>,
      <<"S10,CASE:33,SRC1:432,some_source;T0043,CASE:17,SRC1:634,whatever2;">>,
      <<"S10,CASE:33,SRC1:432,some_source;T0043,CASE:17,SRC1:634,whatever2;">>,
      <<"S10,CASE:33,SRC1:432,some_source;T0043,CASE:17,SRC1:634,whatever2;">>,
      <<"S10,CASE:33,SRC1:432,some_source;T0043,CASE:17,SRC1:634,whatever2;">>,
      <<"S10,CASE:33,SRC1:432,some_source;T0043,CASE:17,SRC1:634,whatever2;">>,
      <<"S10,CASE:33,SRC1:432,some_source;T0043,CASE:17,SRC1:634,whatever2;">>,
      <<"S10,CASE:33,SRC1:432,some_source;T0043,CASE:17,SRC1:634,whatever2;">>,
      <<"S10,CASE:33,SRC1:432,some_source;T0043,CASE:17,SRC1:634,whatever2;">>,
      <<"S10,CASE:33,SRC1:432,some_source;T0043,CASE:17,SRC1:634,whatever2;">>,
      <<"S10,CASE:33,SRC1:432,some_source;T0043,CASE:17,SRC1:634,whatever2;">>,
      <<"S10,CASE:33,SRC1:432,some_source;T0043,CASE:17,SRC1:634,whatever2;">>,
      <<"S10,CASE:33,SRC1:432,some_source;T0043,CASE:17,SRC1:634,whatever2;">>,
      <<"S10,CASE:33,SRC1:432,some_source;T0043,CASE:17,SRC1:634,whatever2;">>,
      <<"S10,CASE:33,SRC1:432,some_source;T0043,CASE:17,SRC1:634,whatever2;">>,
      <<"S10,CASE:33,SRC1:432,some_source;T0043,CASE:17,SRC1:634,whatever2;">>,
      <<"S10,CASE:33,SRC1:432,some_source;T0043,CASE:17,SRC1:634,whatever2;">>,
      <<"S10,CASE:33,SRC1:432,some_source;T0043,CASE:17,SRC1:634,whatever2;">>,
      <<"S10,CASE:33,SRC1:432,some_source;T0043,CASE:17,SRC1:634,whatever2;">>,
      <<"S10,CASE:33,SRC1:432,some_source;T0043,CASE:17,SRC1:634,whatever2;">>,
      <<"S10,CASE:33,SRC1:432,some_source;T0043,CASE:17,SRC1:634,whatever2;">>,
      <<"S10,CASE:33,SRC1:432,some_source;T0043,CASE:17,SRC1:634,whatever2;">>,
      <<"S10,CASE:33,SRC1:432,some_source;T0043,CASE:17,SRC1:634,whatever2;">>,
      <<"S10,CASE:33,SRC1:432,some_source;T0043,CASE:17,SRC1:634,whatever2;">>,
      <<"S10,CASE:33,SRC1:432,some_source;T0043,CASE:17,SRC1:634,whatever2;">>,
      <<"S10,CASE:33,SRC1:432,some_source;T0043,CASE:17,SRC1:634,whatever2;">>,
      <<"S10,CASE:33,SRC1:432,some_source;T0043,CASE:17,SRC1:634,whatever2;">>,
      <<"S10,CASE:33,SRC1:432,some_source;T0043,CASE:17,SRC1:634,whatever2;">>,
      <<"S10,CASE:33,SRC1:432,some_source;T0043,CASE:17,SRC1:634,whatever2;">>,
      <<"S0010,CASE:14,SRC1:432,puuhhhh;T09043,CASE:00,SRC1:2225,helloparser)">>
   ]).

test() ->
   timer:tc(?MODULE, parse, [def()]).