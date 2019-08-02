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
-define(IT_PREFIX, "IT").
-define(DP_PREFIX, "DP").
-define(PP_PREFIX, "PP").
-define(E_PREFIX, "E"). %% maybe L

parse(BinData) ->
   LinesAll = to_lines(BinData),
%%   lager:notice("Lines: ~p",[LinesAll]),
   [_Timestamp|Lines] = LinesAll,
   [<<_DateTime:24/binary, FirstLine/binary>>|RLines] = Lines,
   DataLines = [FirstLine|RLines],
%%   lager:notice("DataLines are: ~p",[DataLines]),
   Res = parse_lines(DataLines),
%%   lager:info("lineCount: ~p~n",[length(Lines)]),
%%   {Timestamp, jsx:encode(Res)}.
   jsx:encode(Res).

to_lines(BinData) ->
   binary:split(BinData, ?LINE_DELIMITER, [global, trim_all]).

parse_lines(Lines) when is_list(Lines) ->
   parse_lines(Lines,
      #{<<"sources">> => [],<<"targets">> => [], <<"its">> => [],
         <<"dps">> => [],<<"pps">> => [], <<"es">> => []}).

parse_lines([], Acc) ->
   Acc;
parse_lines([Line|Rest], Acc) ->
   NewAcc = line(to_fields(Line), Acc),
   parse_lines(Rest, NewAcc).

to_fields(LineData) ->
   binary:split(LineData, ?FIELD_DELIMITER, [global, trim_all]).

%% line with source definition
%% <<"S00,TRAC:2001,SCAN:2001,SEQN:,TARG:,ATTR:20,TRG1:3072,TRG2:,TRG3:,TRG4:,TRG5:;">>
line([<< ?SRC_PREFIX, Source/binary>> | Fields], Acc=#{<<"sources">> := Srcs}) ->
   NewMap = line_src_e_pp(Fields, #{<<"source">> => Source}),
   Acc#{<<"sources">> => [NewMap|Srcs]};

%% line with target definition
line([<< ?TARG_PREFIX, Target/binary>> |Fields], Acc = #{<<"targets">> := Targets}) ->
   NewMap = line_tgt_it_dp(Fields, #{<<"target">> => Target}),
   Acc#{<<"targets">> => [NewMap|Targets]};
%% line with IT definition
%% IT1,TRAC:3062,SCAN:,TEMP:,SEQN:,CASE:,ATTR:0
line([<< ?IT_PREFIX, IT/binary>> |Fields], Acc = #{<<"its">> := Its}) ->
   NewMap = line_tgt_it_dp(Fields, #{<<"it">> => IT}),
   Acc#{<<"its">> => [NewMap|Its]};
%% line with DP definition
%% DP1,TRAC:3068,SCAN:3068,TEMP:,SEQN:1,CASE:3100,ATTR:0
line([<< ?DP_PREFIX, DP/binary>> |Fields], Acc = #{<<"dps">> := Dps}) ->
   NewMap = line_tgt_it_dp(Fields, #{<<"dp">> => DP}),
   Acc#{<<"dps">> => [NewMap|Dps]};
%% line with E definition
%% E01,TRAC:,SCAN:,SEQN:,TARG:,ATTR:0,TRG1:,TRG2:,TRG3:,TRG4:,TRG5:;
line([<< ?E_PREFIX, E/binary>> | Fields], Acc=#{<<"es">> := Es}) ->
   NewMap = line_src_e_pp(Fields, #{<<"e">> => E}),
   Acc#{<<"es">> => [NewMap|Es]};
%% line with PP definition
%% PP1,TRAC:2071,SCAN:2071,SEQN:1,TARG:1,ATTR:16,TRG1:3068,TRG2:,TRG3:,TRG4:,TRG5:;
line([<< ?PP_PREFIX, PP/binary>> | Fields], Acc=#{<<"pps">> := PPs}) ->
   NewMap = line_src_e_pp(Fields, #{<<"pp">> => PP}),
   Acc#{<<"pps">> => [NewMap|PPs]}.

line_src_e_pp(Fields, Map)  ->

   [<<"TRAC:", Trac/binary>>,
   <<"SCAN:", Scan/binary>>,
   <<"SEQN:", SeqN/binary>>,
   <<"TARG:", Targ/binary>>,
   <<"ATTR:", Attr/binary>>,
   <<"TRG1:", Trg1/binary>>,
   <<"TRG2:", Trg2/binary>>,
   <<"TRG3:", Trg3/binary>>,
   <<"TRG4:", Trg4/binary>>,
   <<"TRG5:", Trg5/binary>>] = Fields,
      Map#{ <<"trac">> => Trac,
         <<"scan">> => Scan, <<"seqn">> => int_or_empty(SeqN),
         <<"targ">> => Targ, <<"attr">> => Attr,
         <<"trg1">> => Trg1, <<"trg2">> => Trg2,
         <<"trg3">> => Trg3, <<"trg4">> => Trg4,
         <<"trg5">> => Trg5}.


line_tgt_it_dp(Fields, Map) ->
   [<<"TRAC:", Trac/binary>>,
      <<"SCAN:", Scan/binary>>,
      <<"TEMP:", Temp/binary>>, %% empty or !|? int
      <<"SEQN:", SeqN/binary>>, %% empty or int
      <<"CASE:", Case/binary>>, %% emtpy or int
      <<"ATTR:", Attr/binary>>] = Fields,
   Map#{<<"trac">> => Trac,
      <<"scan">> => Scan, <<"temp">> => Temp,
      <<"seqn">> => int_or_empty(SeqN), <<"case">> => int_or_empty(Case),
      <<"attr">> => Attr}.

int_or_empty(<<>>) -> <<>>;
int_or_empty(Int) -> binary_to_integer(Int).

%% test
def() ->
   iolist_to_binary(
      [<<"2019-08-01 17:33:58.899;">>, %% receive-time on client local time
         <<"19.08.01  17:33:44,867  ">>, %% utc plc send-time
         <<"T00,TRAC:3072,SCAN:3072,TEMP:,SEQN:,CASE:,ATTR:8;">>,
         <<"T01,TRAC:2005,SCAN:2005,TEMP:,SEQN:3,CASE:,ATTR:0;">>,
         <<"T02,TRAC:,SCAN:,TEMP:,SEQN:,CASE:,ATTR:0;">>,
         <<"T03,TRAC:2066,SCAN:2066,TEMP:,SEQN:4,CASE:3220,ATTR:0;">>,
         <<"T04,TRAC:,SCAN:,TEMP:,SEQN:,CASE:4031,ATTR:0;">>,
         <<"T05,TRAC:,SCAN:,TEMP:,SEQN:,CASE:5021,ATTR:0;">>,
         <<"T06,TRAC:2022,SCAN:2022,TEMP:,SEQN:5,CASE:9999,ATTR:0;">>,
         <<"T07,TRAC:,SCAN:,TEMP:,SEQN:,CASE:6,ATTR:0;">>,
         <<"T08,TRAC:2064,SCAN:2064,TEMP:,SEQN:6,CASE:9999,ATTR:0;">>,
         <<"T09,TRAC:,SCAN:,TEMP:,SEQN:,CASE:6,ATTR:0;">>,
         <<"T10,TRAC:3062,SCAN:,TEMP:,SEQN:7,CASE:9999,ATTR:0;">>,
         <<"T11,TRAC:,SCAN:,TEMP:,SEQN:,CASE:16,ATTR:0;">>,
         <<"T12,TRAC:,SCAN:,TEMP:,SEQN:,CASE:4,ATTR:0;">>,
         <<"T13,TRAC:,SCAN:,TEMP:,SEQN:,CASE:3,ATTR:0;">>,
         <<"T14,TRAC:2004,SCAN:2004,TEMP:,SEQN:8,CASE:9999,ATTR:0;">>,
         <<"T15,TRAC:2057,SCAN:2057,TEMP:,SEQN:,CASE:1,ATTR:16;">>,
         <<"IT1,TRAC:3062,SCAN:,TEMP:,SEQN:,CASE:,ATTR:0;">>,
         <<"DP1,TRAC:3068,SCAN:3068,TEMP:,SEQN:1,CASE:3100,ATTR:0;">>,
         <<"DP2,TRAC:2051,SCAN:2051,TEMP:,SEQN:2,CASE:,ATTR:0;">>,
         <<"S00,TRAC:2001,SCAN:2001,SEQN:,TARG:,ATTR:20,TRG1:3072,TRG2:,TRG3:,TRG4:,TRG5:;">>,
         <<"S01,TRAC:2053,SCAN:,SEQN:3,TARG:1,ATTR:16,TRG1:2005,TRG2:,TRG3:,TRG4:,TRG5:;">>,
         <<"S02,TRAC:,SCAN:,SEQN:,TARG:,ATTR:0,TRG1:,TRG2:,TRG3:,TRG4:,TRG5:;">>,
         <<"S03,TRAC:2017,SCAN:,SEQN:4,TARG:1,ATTR:16,TRG1:2066,TRG2:,TRG3:,TRG4:,TRG5:;">>,
         <<"S06,TRAC:3061,SCAN:,SEQN:5,TARG:1,ATTR:16,TRG1:2022,TRG2:,TRG3:,TRG4:,TRG5:;">>,
         <<"S07,TRAC:2012,SCAN:,SEQN:6,TARG:1,ATTR:16,TRG1:2064,TRG2:,TRG3:,TRG4:,TRG5:;">>,
         <<"S08,TRAC:3074,SCAN:,SEQN:7,TARG:1,ATTR:16,TRG1:3062,TRG2:,TRG3:,TRG4:,TRG5:;">>,
         <<"S09,TRAC:2069,SCAN:,SEQN:8,TARG:1,ATTR:16,TRG1:2004,TRG2:,TRG3:,TRG4:,TRG5:;">>,
         <<"S10,TRAC:3064,SCAN:,SEQN:9,TARG:1,ATTR:16,TRG1:2057,TRG2:,TRG3:,TRG4:,TRG5:;">>,
         <<"S11,TRAC:,SCAN:,SEQN:,TARG:,ATTR:0,TRG1:,TRG2:,TRG3:,TRG4:,TRG5:;">>,
         <<"E01,TRAC:,SCAN:,SEQN:,TARG:,ATTR:0,TRG1:,TRG2:,TRG3:,TRG4:,TRG5:;">>,
         <<"PP1,TRAC:2071,SCAN:2071,SEQN:1,TARG:1,ATTR:16,TRG1:3068,TRG2:,TRG3:,TRG4:,TRG5:;">>,
         <<"PP2,TRAC:2058,SCAN:2058,SEQN:2,TARG:1,ATTR:16,TRG1:2051,TRG2:,TRG3:,TRG4:,TRG5:;">>]).

def1() -> iolist_to_binary([
%%   ,
         <<"2019-08-01 17:35:58.930;">>,
         <<"19.08.01  17:35:44,899  ">>,
         <<"T00,TRAC:3072,SCAN:3072,TEMP:,SEQN:,CASE:,ATTR:8;">>,
         <<"T01,TRAC:2005,SCAN:2005,TEMP:,SEQN:3,CASE:,ATTR:0;">>,
         <<"T02,TRAC:,SCAN:,TEMP:,SEQN:,CASE:,ATTR:0;">>,
         <<"T03,TRAC:2066,SCAN:2066,TEMP:,SEQN:4,CASE:3220,ATTR:0;">>,
         <<"T04,TRAC:,SCAN:,TEMP:,SEQN:,CASE:4031,ATTR:0;">>,
         <<"T05,TRAC:,SCAN:,TEMP:,SEQN:,CASE:5021,ATTR:0;">>,
         <<"T06,TRAC:2022,SCAN:2022,TEMP:,SEQN:5,CASE:9999,ATTR:0;">>,
         <<"T07,TRAC:,SCAN:,TEMP:,SEQN:,CASE:6,ATTR:0;">>,
         <<"T08,TRAC:2064,SCAN:2064,TEMP:,SEQN:6,CASE:9999,ATTR:0;">>,
         <<"T09,TRAC:,SCAN:,TEMP:,SEQN:,CASE:6,ATTR:0;">>,
         <<"T10,TRAC:3062,SCAN:,TEMP:,SEQN:7,CASE:9999,ATTR:0;">>,
         <<"T11,TRAC:,SCAN:,TEMP:,SEQN:,CASE:16,ATTR:0;">>,
         <<"T12,TRAC:,SCAN:,TEMP:,SEQN:,CASE:4,ATTR:0;">>,
         <<"T13,TRAC:,SCAN:,TEMP:,SEQN:,CASE:3,ATTR:0;">>,
         <<"T14,TRAC:2004,SCAN:2004,TEMP:,SEQN:8,CASE:9999,ATTR:0;">>,
         <<"T15,TRAC:2057,SCAN:2057,TEMP:,SEQN:,CASE:1,ATTR:16;">>,
         <<"IT1,TRAC:3062,SCAN:,TEMP:,SEQN:,CASE:,ATTR:0;">>,
         <<"DP1,TRAC:3068,SCAN:3068,TEMP:,SEQN:1,CASE:3100,ATTR:0;">>,
         <<"DP2,TRAC:2051,SCAN:2051,TEMP:,SEQN:2,CASE:,ATTR:0;">>,
         <<"S00,TRAC:2001,SCAN:2001,SEQN:,TARG:,ATTR:20,TRG1:3072,TRG2:,TRG3:,TRG4:,TRG5:;">>,
         <<"S01,TRAC:2053,SCAN:,SEQN:3,TARG:1,ATTR:16,TRG1:2005,TRG2:,TRG3:,TRG4:,TRG5:;">>,
         <<"S02,TRAC:,SCAN:,SEQN:,TARG:,ATTR:0,TRG1:,TRG2:,TRG3:,TRG4:,TRG5:;">>,
         <<"S03,TRAC:2017,SCAN:,SEQN:4,TARG:1,ATTR:16,TRG1:2066,TRG2:,TRG3:,TRG4:,TRG5:;">>,
         <<"S06,TRAC:3061,SCAN:,SEQN:5,TARG:1,ATTR:16,TRG1:2022,TRG2:,TRG3:,TRG4:,TRG5:;">>,
         <<"S07,TRAC:2012,SCAN:,SEQN:6,TARG:1,ATTR:16,TRG1:2064,TRG2:,TRG3:,TRG4:,TRG5:;">>,
         <<"S08,TRAC:3074,SCAN:,SEQN:7,TARG:1,ATTR:16,TRG1:3062,TRG2:,TRG3:,TRG4:,TRG5:;">>,
         <<"S09,TRAC:2069,SCAN:,SEQN:8,TARG:1,ATTR:16,TRG1:2004,TRG2:,TRG3:,TRG4:,TRG5:;">>,
         <<"S10,TRAC:3064,SCAN:,SEQN:9,TARG:1,ATTR:16,TRG1:2057,TRG2:,TRG3:,TRG4:,TRG5:;">>,
         <<"S11,TRAC:,SCAN:,SEQN:,TARG:,ATTR:0,TRG1:,TRG2:,TRG3:,TRG4:,TRG5:;">>,
         <<"E01,TRAC:,SCAN:,SEQN:,TARG:,ATTR:0,TRG1:,TRG2:,TRG3:,TRG4:,TRG5:;">>,
         <<"PP1,TRAC:2071,SCAN:2071,SEQN:1,TARG:1,ATTR:16,TRG1:3068,TRG2:,TRG3:,TRG4:,TRG5:;">>,
         <<"PP2,TRAC:2058,SCAN:2058,SEQN:2,TARG:1,ATTR:16,TRG1:2051,TRG2:,TRG3:,TRG4:,TRG5:;">>]
   ).
test() ->
%%   LinesAll = to_lines(def()),
%%   [Timestamp|Lines] = LinesAll,
%%   [<<_DateTime:23/binary, FirstLine/binary>>|RLines] = Lines,
%%   [FirstLine|RLines].
   {T, Res} = timer:tc(?MODULE, parse, [def()]),
   lager:info("Time (micros) needed: ~p~nJSON: ~s",[T, binary_to_list(Res)]),
   {T1, Res1} = timer:tc(?MODULE, parse, [def1()]),
   lager:info("Time (micros) needed: ~p~nJSON: ~s",[T1, binary_to_list(Res1)]).