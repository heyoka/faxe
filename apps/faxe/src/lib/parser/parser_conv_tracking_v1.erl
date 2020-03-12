%%%-------------------------------------------------------------------
%%% @author heyoka
%%% @copyright (C) 2019, <COMPANY>
%%% @doc
%%% parser for the conveyor tracking protocol version1
%%% @end
%%% Created : 01. Aug 2019 16:53
%%%-------------------------------------------------------------------
-module(parser_conv_tracking_v1).
-author("heyoka").

-behavior(binary_msg_parser).

%% API
-export([parse/1, test_bitmask/1, parse_datetime/1, maybe_disambiguate/1]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(PARSER_VERSION, 1).
-define(TGW_DATAFORMAT, <<"03.002">>).

%% byte-length of timestamp inserted by the PLC
-define(PLC_DATETIME_LENGTH, 24).


-define(LINE_DELIMITER, <<";">>).
-define(FIELD_DELIMITER, <<",">>).
-define(VALUE_DELIMITER, <<":">>).
-define(SRC_PREFIX, "S"). %% source
-define(TARG_PREFIX, "T"). %% target
-define(IT_PREFIX, "IT").
-define(DP_PREFIX, "DP").
-define(PP_PREFIX, "PP").
-define(ET_PREFIX_IGNORE, "ET"). %% will be ignored
-define(E_PREFIX, "E"). %% maybe L

%% bitmask values for ATTR field
-define(TARGET_BITMASK_VAL, [
   <<"goto_putface">>,
   <<"no_read">>,
   <<"not_scanned">>,
   <<"no_source">>,
   <<"goto_sequencer">>,
   <<"forced_target">>,
   <<"goto_exit">>,
   <<"recirculate">>]).

-define(SOURCE_BITMASK_VAL, [
   <<"">>,
   <<"no_read">>,
   <<"">>,
   <<"source_processed">>,
   <<"no_target_found">>,
   <<"goto_exit">>,
   <<"goto_bypass">>,
   <<"recirculate">>]).

parse(BinData) ->
   Timestamp = faxe_time:now(),
   ets:insert(parser_prev_table(), ets:tab2list(parser_table())),
   ets:delete_all_objects(parser_table()),
   %% split lines
   LinesAll = to_lines(BinData),


   %% can be used, if the message's datetime is valid and synced
%%   {Ts, DataLines} =
%%   case LinesAll of
%%      [<<DateTime:?PLC_DATETIME_LENGTH/binary, FirstLine/binary>>|RLines] ->
%%         Ts = parse_datetime(DateTime),
%%         {Ts, [FirstLine|RLines]};
%%      [DateTime | RLines] ->
%%         Ts = parse_datetime_new(DateTime),
%%         {Ts, RLines}
%%   end,
   DataLines =
   case LinesAll of
      [<<_DateTime:?PLC_DATETIME_LENGTH/binary, FirstLine/binary>>|RLines] ->
         [FirstLine|RLines];
      [_DateTime | RLines] ->
         RLines
   end,
   %% parse the lines
   Res = parse_lines(DataLines),
   %% no check for inconsistencies
   DisambRes = maybe_disambiguate(Res),

   {Timestamp, ?TGW_DATAFORMAT, ?PARSER_VERSION, DisambRes}.
%% test version, were we use the time in the plc message (but the time is not valid at the moment)
%%   {Ts, ?TGW_DATAFORMAT, ?PARSER_VERSION, DisambRes}.


%% if there is no previous parser data OR if there are obviously no duplicate values for "trac" ->
%% do nothing at all !
maybe_disambiguate(Result) ->
   case ets:tab2list(parser_prev_table()) of
      [] -> Result;
      _L ->
         TList = lists:filter(fun({_P, T}) -> T /= <<>> end, ets:tab2list(parser_table())),
         {_Positions, Tracs} = lists:unzip(TList),
         %% check if there are duplicates
         case length(Tracs) == length(lists:usort(Tracs)) of
            true -> Result;
            false -> disambiguate(Result, TList)
         end
   end.

disambiguate(ResMap, TList) ->
   %% log from last parser run
   LastTime = ets:tab2list(parser_prev_table()),
   %% lets filter out double "trac" values, that is what we are interested in
   F =
   fun({_Pos, Trac}, {Seen, Doubles}) ->
      NewDoubles =
      case lists:member(Trac, Seen) of
         true -> %% we have a second entry here
            [Trac|Doubles];
         false -> Doubles
      end,
      {[Trac|Seen], NewDoubles}
   end,
   {_Seen, Doubles} = lists:foldl(F, {[], []}, TList),
   %% use doubles list to reduce the parser data
   TheDoubles = lists:filter(fun({_Po, Tr}) -> lists:member(Tr, Doubles) end, TList),

   %% check these with last parser values to get Actions list
   FunEval =
   fun({DPos, DTrac}, Acc) ->
      case proplists:get_value(DPos, LastTime) of
         DTrac -> %% we were here last time, and we know we are double, so set entry to <<>>
            ets:delete_object(parser_table(), {DPos, DTrac}),
            [DPos|Acc];
         _Else -> %% undefined or other entry
            Acc
      end
   end,
   Actions = lists:foldl(FunEval, [], TheDoubles),
%%   lager:notice("The Actions are: ~p",[Actions]),
   %% execute the actions on the parsed list of maps
   #{<<"sources">> := Sources, <<"targets">> := Targets} = ResMap,
   %% run over every entry from the parsed data
   AllPos = lists:concat([Sources, Targets]),
   Disamb =
   fun(#{<<"pos">> := Pos} = El, #{<<"sources">> := Srcs, <<"targets">> := Trgts} = Acc) ->
      NewEntry =
      case lists:member(Pos, Actions) of
         true  ->
            El#{<<"trac">> => <<>>, <<"scan">> => <<>>};
         false ->
            El
      end,
      %% with the presence of the field "targ" we decide if the entry is source or target
      case maps:is_key(<<"targ">>, NewEntry) of
         true -> Acc#{<<"sources">> => [NewEntry|Srcs]};
         false -> Acc#{<<"targets">> => [NewEntry|Trgts]}
      end
   end,
   lists:foldl(Disamb, #{<<"sources">> => [], <<"targets">> => []}, AllPos).


to_lines(BinData) ->
   binary:split(BinData, ?LINE_DELIMITER, [global, trim_all]).

parse_lines(Lines) when is_list(Lines) ->
   parse_lines(Lines,
      #{<<"sources">> => [],<<"targets">> => []}).

parse_lines([], Acc) ->
   Acc;
parse_lines([Line|Rest], Acc) ->
   NewAcc = line(to_fields(Line), Acc),
   parse_lines(Rest, NewAcc).

to_fields(LineData) ->
   binary:split(LineData, ?FIELD_DELIMITER, [global, trim_all]).

%% line with source definition
%% <<"S00,TRAC:2001,SCAN:2001,SEQN:,TARG:,ATTR:20,TRG1:3072,TRG2:,TRG3:,TRG4:,TRG5:;">>
line([<< ?SRC_PREFIX, _Source/binary>> = Pos | Fields], Acc=#{<<"sources">> := Srcs}) ->
   NewMap = line_src_e_pp(Pos, Fields),
   Acc#{<<"sources">> => [NewMap|Srcs]};

%% line with target definition
line([<< ?TARG_PREFIX, _Target/binary>> = Pos |Fields], Acc = #{<<"targets">> := Targets}) ->
   NewMap = line_tgt_it_dp(Pos, Fields),
   Acc#{<<"targets">> => [NewMap|Targets]};
%% line with IT definition
%% IT1,TRAC:3062,SCAN:,TEMP:,SEQN:,CASE:,ATTR:0
line([<< ?IT_PREFIX, _IT/binary>> = Pos |Fields], Acc = #{<<"targets">> := Targets}) ->
   NewMap = line_tgt_it_dp(Pos, Fields),
   Acc#{<<"targets">> => [NewMap|Targets]};
%% line with DP definition
%% DP1,TRAC:3068,SCAN:3068,TEMP:,SEQN:1,CASE:3100,ATTR:0
line([<< ?DP_PREFIX, _DP/binary>> = Pos |Fields], Acc = #{<<"targets">> := Targets}) ->
   NewMap = line_tgt_it_dp(Pos, Fields),
   Acc#{<<"targets">> => [NewMap|Targets]};
%% line with ET definition
%% will be ignored here
line([<< ?ET_PREFIX_IGNORE, _E/binary>> = _Pos| _Fields], Acc) ->
   Acc;
%% line with E definition
%% E01,TRAC:,SCAN:,SEQN:,TARG:,ATTR:0,TRG1:,TRG2:,TRG3:,TRG4:,TRG5:;
line([<< ?E_PREFIX, _E/binary>> = Pos| Fields], Acc=#{<<"sources">> := Srcs}) ->
   NewMap = line_src_e_pp(Pos, Fields),
   Acc#{<<"sources">> => [NewMap|Srcs]};
%% line with PP definition
%% PP1,TRAC:2071,SCAN:2071,SEQN:1,TARG:1,ATTR:16,TRG1:3068,TRG2:,TRG3:,TRG4:,TRG5:;
line([<< ?PP_PREFIX, _PP/binary>> = Pos| Fields], Acc=#{<<"sources">> := Srcs}) ->
   NewMap = line_src_e_pp(Pos, Fields),
   Acc#{<<"sources">> => [NewMap|Srcs]}.

%%[<<"N:0">>,<<"CASE:0">>,<<"ATTR:0">>]

%% SOURCE, E, PP lines
line_src_e_pp(Pos, Fields0)  ->
   Fields = src_match(Fields0),
   [<<"TRAC:", Trac/binary>>,
      <<"SCAN:", Scan/binary>>,
      <<"SEQN:", SeqN/binary>>,
      <<"ATTR:", Attr/binary>>,
      <<"TRG1:", Targ1/binary>>,
      <<"TRG2:", Targ2/binary>>,
      <<"TRG3:", Targ3/binary>>,
      <<"TRG4:", Targ4/binary>>,
      <<"TRG5:", Targ5/binary>>] = Fields,
   ets:insert(parser_table(), {Pos, Trac}),
   #{<<"pos">> => Pos,
      <<"trac">> => Trac,
      <<"scan">> => Scan, <<"seqn">> => int_or_null(SeqN),
      <<"attr">> => eval_bitarray(int_or_empty(Attr), ?SOURCE_BITMASK_VAL),
      <<"targ">> => lists:filter(fun(E) -> E /= <<>> end, [Targ1, Targ2, Targ3, Targ4, Targ5])}.

%% kick out "TARG" as it may not be there and is not used anyway
src_match([
   <<"TRAC:", Trac/binary>>,
   <<"SCAN:", Scan/binary>>,
   <<"SEQN:", SeqN/binary>>,
   <<"TARG:", _Targ/binary>>,
   <<"ATTR:", _/binary>>,
   <<"TRG1:", _/binary>>,
   <<"TRG2:", _/binary>>,
   <<"TRG3:", _/binary>>,
   <<"TRG4:", _/binary>>,
   <<"TRG5:", _/binary>>] = Fields) ->

   [<<"TRAC:", Trac/binary>>,
      <<"SCAN:", Scan/binary>>,
      <<"SEQN:", SeqN/binary>>] ++ lists:nthtail(4, Fields);

src_match(Fields) -> Fields.



%% TARGET, IT, DP lines
line_tgt_it_dp(Pos, Fields) ->
   [<<"TRAC:", Trac/binary>>,
      <<"SCAN:", Scan/binary>>,
      <<"TEMP:", Temp/binary>>, %% empty or !|? int
      <<"SEQN:", SeqN/binary>>, %% empty or int
      <<"CASE:", Case/binary>>, %% emtpy or int
      <<"ATTR:", Attr/binary>>] = Fields,
   ets:insert(parser_table(), {Pos, Trac}),
   #{<<"pos">> => Pos, <<"trac">> => Trac,
      <<"scan">> => Scan, <<"temp">> => Temp,
      <<"seqn">> => int_or_null(SeqN), <<"case">> => int_or_null(Case),
      <<"attr">> => eval_bitarray(int_or_empty(Attr), ?TARGET_BITMASK_VAL)}.

int_or_null(<<>>) -> null;
int_or_null(Int) when is_binary(Int) -> binary_to_integer(Int).

int_or_empty(<<>>) -> <<>>;
int_or_empty(Int) -> binary_to_integer(Int).

%% bitmask code
test_bitmask(V) ->
   test_bitmask(V, ?SOURCE_BITMASK_VAL).
test_bitmask(V, List) ->
   eval_bitarray(V, List).

eval_bitarray(Int, List) when is_integer(Int) ->
   eval_bitarray(<<Int>>, List);
eval_bitarray(Bitstring, List) when is_bitstring(Bitstring) ->
   bitm(Bitstring, List, []).
%% end of list
bitm(_Bits, [], Gathered) ->
   lists:reverse(Gathered);
%% end of bitstring
bitm(<<>>, _Others, Gathered) ->
   lists:reverse(Gathered);
bitm(<<Bit:1, Rest/bitstring>>, [Item | Others], Gathered) ->
   NextR = if Bit band 1 /= 0 -> [Item | Gathered]; true -> Gathered end,
   bitm(Rest, Others, NextR).

-spec parse_datetime(binary()) -> faxe_time:timestamp().
parse_datetime(DTBin) ->
   lager:notice("datetime: ~p",[DTBin]),
   time_format:conv_dt_to_ms(DTBin).

%%parse_datetime_new(DTBin) ->
%%   lager:notice("datetime: ~p",[DTBin]),
%%   %% add "Z" to get UTC time
%%   time_format:rfc3339_to_ms(<<DTBin/binary, "Z">>).

%%%%%%%%%%%%%%%%%%% parser tables %%%%%%%%%%%%%%%%%%%%%%
%% parser table handles stored in process dictionary
parser_table() ->
   case get(parser) of
      undefined ->
         Tab = ets:new(parser, [bag]),
         put(parser, Tab), Tab;
      Val -> Val
   end.

parser_prev_table() ->
   case get(parser_prev) of
      undefined ->
         Tab = ets:new(parser_prev, [set]),
         put(parser_prev, Tab), Tab;
      Val -> Val
   end.

%%%%%%%%%%%%%%%%%% end %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-ifdef(TEST).
%% test
def() ->
   iolist_to_binary(
      [
         <<"19.08.01  17:33:44,867  ">>, %% utc plc send-time
         <<"T00,TRAC:3072,SCAN:3072,TEMP:,SEQN:,CASE:,ATTR:8;">>,
         <<"T01,TRAC:2005,SCAN:2005,TEMP:,SEQN:3,CASE:,ATTR:0;">>,
         <<"T02,TRAC:,SCAN:,TEMP:,SEQN:,CASE:,ATTR:0;">>,
         <<"T03,TRAC:2066,SCAN:2066,TEMP:,SEQN:4,CASE:3220,ATTR:0;">>,
         <<"T04,TRAC:,SCAN:,TEMP:,SEQN:,CASE:4031,ATTR:0;">>,
         <<"T05,TRAC:,SCAN:,TEMP:,SEQN:,CASE:5021,ATTR:0;">>,
         <<"T06,TRAC:,SCAN:,TEMP:,SEQN:5,CASE:9999,ATTR:0;">>,
         <<"T07,TRAC:2022,SCAN:,TEMP:,SEQN:,CASE:6,ATTR:0;">>,
         <<"T08,TRAC:2064,SCAN:2064,TEMP:,SEQN:6,CASE:9999,ATTR:0;">>,
         <<"T09,TRAC:,SCAN:,TEMP:,SEQN:,CASE:6,ATTR:0;">>,
         <<"T10,TRAC:3062,SCAN:,TEMP:,SEQN:7,CASE:9999,ATTR:0;">>,
         <<"T11,TRAC:,SCAN:,TEMP:,SEQN:,CASE:16,ATTR:0;">>,
         <<"T12,TRAC:,SCAN:,TEMP:,SEQN:,CASE:4,ATTR:0;">>,
         <<"T13,TRAC:,SCAN:,TEMP:,SEQN:,CASE:3,ATTR:0;">>,
         <<"T14,TRAC:2004,SCAN:2004,TEMP:,SEQN:8,CASE:9999,ATTR:0;">>,
         <<"T15,TRAC:2057,SCAN:2057,TEMP:,SEQN:,CASE:1,ATTR:16;">>,
         <<"IT1,TRAC:,SCAN:,TEMP:,SEQN:,CASE:,ATTR:0;">>,
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
defnew() ->
   iolist_to_binary(
      [
         <<"19.08.01  17:33:44,867  ">>, %% utc plc send-time
         <<"T00,TRAC:3072,SCAN:3072,TEMP:,SEQN:,CASE:,ATTR:8;">>,
         <<"T01,TRAC:2005,SCAN:2005,TEMP:,SEQN:3,CASE:,ATTR:0;">>,
         <<"T02,TRAC:,SCAN:,TEMP:,SEQN:,CASE:,ATTR:0;">>,
         <<"T03,TRAC:2066,SCAN:2066,TEMP:,SEQN:4,CASE:3220,ATTR:0;">>,
         <<"T04,TRAC:,SCAN:,TEMP:,SEQN:,CASE:4031,ATTR:0;">>,
         <<"T05,TRAC:,SCAN:,TEMP:,SEQN:,CASE:5021,ATTR:0;">>,
         <<"T06,TRAC:2022,SCAN:2022,TEMP:,SEQN:5,CASE:9999,ATTR:0;">>,
         <<"T07,TRAC:2022,SCAN:2022,TEMP:,SEQN:,CASE:6,ATTR:0;">>,
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
   <<"19.08.01  17:35:44,899  ">>,
   <<"T00,TRAC:3072,SCAN:3072,TEMP:,SEQN:,CASE:,ATTR:8;">>,
   <<"T09,TRAC:,SCAN:,TEMP:,SEQN:,CASE:6,ATTR:0;">>,
   <<"T10,TRAC:3062,SCAN:,TEMP:,SEQN:7,CASE:9999,ATTR:0;">>,
   <<"T15,TRAC:2057,SCAN:2057,TEMP:,SEQN:,CASE:1,ATTR:16;">>,
   <<"IT1,TRAC:3062,SCAN:,TEMP:,SEQN:,CASE:,ATTR:0;">>,
   <<"DP1,TRAC:3068,SCAN:3068,TEMP:,SEQN:1,CASE:3100,ATTR:0;">>,
   <<"DP2,TRAC:2051,SCAN:2051,TEMP:,SEQN:2,CASE:,ATTR:0;">>,
%%    S00,TRAC:,SCAN:,SEQN:0,ATTR:0,TRG1:,TRG2:,TRG3:,TRG4:,TRG5:
   <<"S00,TRAC:2001,SCAN:2001,SEQN:,ATTR:20,TRG1:3072,TRG2:,TRG3:,TRG4:,TRG5:;">>,
   <<"S10,TRAC:3064,SCAN:,SEQN:9,TARG:1,ATTR:16,TRG1:2057,TRG2:,TRG3:,TRG4:,TRG5:;">>,
   <<"S11,TRAC:,SCAN:,SEQN:,TARG:,ATTR:0,TRG1:,TRG2:,TRG3:,TRG4:,TRG5:;">>,
   <<"E01,TRAC:,SCAN:,SEQN:,TARG:,ATTR:0,TRG1:,TRG2:0000000000E8,TRG3:,TRG4:,TRG5:;">>,
   <<"PP2,TRAC:2058,SCAN:2058,SEQN:2,TARG:1,ATTR:16,TRG1:2051,TRG2:,TRG3:,TRG4:,TRG5:;">>]
).

res_new() ->
   #{<<"sources">> =>
   [#{<<"attr">> =>
   [<<"source_processed">>,<<"goto_exit">>],
      <<"pos">> => <<"S00">>,<<"scan">> => <<"2001">>,
      <<"seqn">> => null,
      <<"targ">> => [<<"3072">>],
      <<"trac">> => <<"2001">>},
      #{<<"attr">> => [<<"source_processed">>],
         <<"pos">> => <<"S10">>,<<"scan">> => <<>>,
         <<"seqn">> => 9,
         <<"targ">> => [<<"2057">>],
         <<"trac">> => <<"3064">>},
      #{<<"attr">> => [],<<"pos">> => <<"S11">>,
         <<"scan">> => <<>>,<<"seqn">> => null,
         <<"targ">> => [],<<"trac">> => <<>>},
      #{<<"attr">> => [],<<"pos">> => <<"E01">>,
         <<"scan">> => <<>>,<<"seqn">> => null,
         <<"targ">> => [<<"0000000000E8">>],
         <<"trac">> => <<>>},
      #{<<"attr">> => [<<"source_processed">>],
         <<"pos">> => <<"PP2">>,<<"scan">> => <<"2058">>,
         <<"seqn">> => 2,
         <<"targ">> => [<<"2051">>],
         <<"trac">> => <<"2058">>}],
      <<"targets">> =>
      [#{<<"attr">> => [<<"goto_sequencer">>],
         <<"case">> => null,<<"pos">> => <<"T00">>,
         <<"scan">> => <<"3072">>,<<"seqn">> => null,
         <<"temp">> => <<>>,<<"trac">> => <<"3072">>},
         #{<<"attr">> => [],<<"case">> => 6,
            <<"pos">> => <<"T09">>,<<"scan">> => <<>>,
            <<"seqn">> => null,<<"temp">> => <<>>,
            <<"trac">> => <<>>},
         #{<<"attr">> => [],<<"case">> => 9999,
            <<"pos">> => <<"T10">>,<<"scan">> => <<>>,
            <<"seqn">> => 7,<<"temp">> => <<>>,
            <<"trac">> => <<>>},
         #{<<"attr">> => [<<"no_source">>],
            <<"case">> => 1,<<"pos">> => <<"T15">>,
            <<"scan">> => <<"2057">>,<<"seqn">> => null,
            <<"temp">> => <<>>,<<"trac">> => <<"2057">>},
         #{<<"attr">> => [],<<"case">> => null,
            <<"pos">> => <<"IT1">>,<<"scan">> => <<>>,
            <<"seqn">> => null,<<"temp">> => <<>>,
            <<"trac">> => <<>>},
         #{<<"attr">> => [],<<"case">> => 3100,
            <<"pos">> => <<"DP1">>,<<"scan">> => <<"3068">>,
            <<"seqn">> => 1,<<"temp">> => <<>>,
            <<"trac">> => <<"3068">>},
         #{<<"attr">> => [],<<"case">> => null,
            <<"pos">> => <<"DP2">>,<<"scan">> => <<"2051">>,
            <<"seqn">> => 2,<<"temp">> => <<>>,
            <<"trac">> => <<"2051">>}]}

.


res2() ->
   #{<<"sources">> =>
   [#{<<"attr">> =>
   [<<"source_processed">>,<<"goto_exit">>],
      <<"pos">> => <<"S00">>,<<"scan">> => <<"2001">>,
      <<"seqn">> => null,
      <<"targ">> => [<<"3072">>],
      <<"trac">> => <<"2001">>},
      #{<<"attr">> => [<<"source_processed">>],
         <<"pos">> => <<"S01">>,<<"scan">> => <<>>,
         <<"seqn">> => 3,
         <<"targ">> => [<<"2005">>],
         <<"trac">> => <<"2053">>},
      #{<<"attr">> => [],<<"pos">> => <<"S02">>,
         <<"scan">> => <<>>,<<"seqn">> => null,
         <<"targ">> => [],<<"trac">> => <<>>},
      #{<<"attr">> => [<<"source_processed">>],
         <<"pos">> => <<"S03">>,<<"scan">> => <<>>,
         <<"seqn">> => 4,
         <<"targ">> => [<<"2066">>],
         <<"trac">> => <<"2017">>},
      #{<<"attr">> => [<<"source_processed">>],
         <<"pos">> => <<"S06">>,<<"scan">> => <<>>,
         <<"seqn">> => 5,
         <<"targ">> => [<<"2022">>],
         <<"trac">> => <<"3061">>},
      #{<<"attr">> => [<<"source_processed">>],
         <<"pos">> => <<"S07">>,<<"scan">> => <<>>,
         <<"seqn">> => 6,
         <<"targ">> => [<<"2064">>],
         <<"trac">> => <<"2012">>},
      #{<<"attr">> => [<<"source_processed">>],
         <<"pos">> => <<"S08">>,<<"scan">> => <<>>,
         <<"seqn">> => 7,
         <<"targ">> => [<<"3062">>],
         <<"trac">> => <<"3074">>},
      #{<<"attr">> => [<<"source_processed">>],
         <<"pos">> => <<"S09">>,<<"scan">> => <<>>,
         <<"seqn">> => 8,
         <<"targ">> => [<<"2004">>],
         <<"trac">> => <<"2069">>},
      #{<<"attr">> => [<<"source_processed">>],
         <<"pos">> => <<"S10">>,<<"scan">> => <<>>,
         <<"seqn">> => 9,
         <<"targ">> => [<<"2057">>],
         <<"trac">> => <<"3064">>},
      #{<<"attr">> => [],<<"pos">> => <<"S11">>,
         <<"scan">> => <<>>,<<"seqn">> => null,
         <<"targ">> => [],<<"trac">> => <<>>},
      #{<<"attr">> => [],<<"pos">> => <<"E01">>,
         <<"scan">> => <<>>,<<"seqn">> => null,
         <<"targ">> => [],<<"trac">> => <<>>},
      #{<<"attr">> => [<<"source_processed">>],
         <<"pos">> => <<"PP1">>,<<"scan">> => <<"2071">>,
         <<"seqn">> => 1,
         <<"targ">> => [<<"3068">>],
         <<"trac">> => <<"2071">>},
      #{<<"attr">> => [<<"source_processed">>],
         <<"pos">> => <<"PP2">>,<<"scan">> => <<"2058">>,
         <<"seqn">> => 2,
         <<"targ">> => [<<"2051">>],
         <<"trac">> => <<"2058">>}],
      <<"targets">> =>
      [#{<<"attr">> => [<<"goto_sequencer">>],
         <<"case">> => null,<<"pos">> => <<"T00">>,
         <<"scan">> => <<"3072">>,<<"seqn">> => null,
         <<"temp">> => <<>>,<<"trac">> => <<"3072">>},
         #{<<"attr">> => [],<<"case">> => null,
            <<"pos">> => <<"T01">>,<<"scan">> => <<"2005">>,
            <<"seqn">> => 3,<<"temp">> => <<>>,
            <<"trac">> => <<"2005">>},
         #{<<"attr">> => [],<<"case">> => null,
            <<"pos">> => <<"T02">>,<<"scan">> => <<>>,
            <<"seqn">> => null,<<"temp">> => <<>>,
            <<"trac">> => <<>>},
         #{<<"attr">> => [],<<"case">> => 3220,
            <<"pos">> => <<"T03">>,<<"scan">> => <<"2066">>,
            <<"seqn">> => 4,<<"temp">> => <<>>,
            <<"trac">> => <<"2066">>},
         #{<<"attr">> => [],<<"case">> => 4031,
            <<"pos">> => <<"T04">>,<<"scan">> => <<>>,
            <<"seqn">> => null,<<"temp">> => <<>>,
            <<"trac">> => <<>>},
         #{<<"attr">> => [],<<"case">> => 5021,
            <<"pos">> => <<"T05">>,<<"scan">> => <<>>,
            <<"seqn">> => null,<<"temp">> => <<>>,
            <<"trac">> => <<>>},
         #{<<"attr">> => [],<<"case">> => 9999,
            <<"pos">> => <<"T06">>,<<"scan">> => <<"2022">>,
            <<"seqn">> => 5,<<"temp">> => <<>>,
            <<"trac">> => <<"2022">>},
         #{<<"attr">> => [],<<"case">> => 6,
            <<"pos">> => <<"T07">>,<<"scan">> => <<>>,
            <<"seqn">> => null,<<"temp">> => <<>>,
            <<"trac">> => <<>>},
         #{<<"attr">> => [],<<"case">> => 9999,
            <<"pos">> => <<"T08">>,<<"scan">> => <<"2064">>,
            <<"seqn">> => 6,<<"temp">> => <<>>,
            <<"trac">> => <<"2064">>},
         #{<<"attr">> => [],<<"case">> => 6,
            <<"pos">> => <<"T09">>,<<"scan">> => <<>>,
            <<"seqn">> => null,<<"temp">> => <<>>,
            <<"trac">> => <<>>},
         #{<<"attr">> => [],<<"case">> => 9999,
            <<"pos">> => <<"T10">>,<<"scan">> => <<>>,
            <<"seqn">> => 7,<<"temp">> => <<>>,
            <<"trac">> => <<>>},
         #{<<"attr">> => [],<<"case">> => 16,
            <<"pos">> => <<"T11">>,<<"scan">> => <<>>,
            <<"seqn">> => null,<<"temp">> => <<>>,
            <<"trac">> => <<>>},
         #{<<"attr">> => [],<<"case">> => 4,
            <<"pos">> => <<"T12">>,<<"scan">> => <<>>,
            <<"seqn">> => null,<<"temp">> => <<>>,
            <<"trac">> => <<>>},
         #{<<"attr">> => [],<<"case">> => 3,
            <<"pos">> => <<"T13">>,<<"scan">> => <<>>,
            <<"seqn">> => null,<<"temp">> => <<>>,
            <<"trac">> => <<>>},
         #{<<"attr">> => [],<<"case">> => 9999,
            <<"pos">> => <<"T14">>,<<"scan">> => <<"2004">>,
            <<"seqn">> => 8,<<"temp">> => <<>>,
            <<"trac">> => <<"2004">>},
         #{<<"attr">> => [<<"no_source">>],
            <<"case">> => 1,<<"pos">> => <<"T15">>,
            <<"scan">> => <<"2057">>,<<"seqn">> => null,
            <<"temp">> => <<>>,<<"trac">> => <<"2057">>},
         #{<<"attr">> => [],<<"case">> => null,
            <<"pos">> => <<"IT1">>,<<"scan">> => <<>>,
            <<"seqn">> => null,<<"temp">> => <<>>,
            <<"trac">> => <<"3062">>},
         #{<<"attr">> => [],<<"case">> => 3100,
            <<"pos">> => <<"DP1">>,<<"scan">> => <<"3068">>,
            <<"seqn">> => 1,<<"temp">> => <<>>,
            <<"trac">> => <<"3068">>},
         #{<<"attr">> => [],<<"case">> => null,
            <<"pos">> => <<"DP2">>,<<"scan">> => <<"2051">>,
            <<"seqn">> => 2,<<"temp">> => <<>>,
            <<"trac">> => <<"2051">>}]}
.




i_test() ->
   {T, Res} = timer:tc(?MODULE, parse, [def()]),
   lager:info("Time (micros) needed: ~p~nJSON: ~s",[T, binary_to_list(Res)]),
   {T1, Res1} = timer:tc(?MODULE, parse, [def1()]),
   lager:info("Time (micros) needed: ~p~nJSON: ~s",[T1, Res1]).

parse_test() ->
   {Ts, _Df, _Vs, _Data} = Res = parse(def1()),
   ?assertEqual(Res, {Ts, ?TGW_DATAFORMAT, ?PARSER_VERSION, res_new()}).

parse_disambiguate_test() ->
   parse(def()),
   {Ts, _Df, _Vs, _Data} = Res = parse(defnew()),
   ?assertEqual(Res,
      {Ts, ?TGW_DATAFORMAT, ?PARSER_VERSION, res2()}).

test_bin_lines() -> <<" 1;2;;34;35,767,67; /4365;0:">>.
test_bin_fields() -> <<"ARG:4,BRG:3, ,, SomeField:,AnotherField:hello,KKKRF">>.

to_lines_test() ->
   Res = [<<" 1">>,<<"2">>,<<"34">>,<<"35,767,67">>,<<" /4365">>,<<"0:">>],
   ?assertEqual(to_lines(test_bin_lines()), Res).

to_fields_test() ->
   Res = [<<"ARG:4">>,<<"BRG:3">>,<<" ">>,<<" SomeField:">>, <<"AnotherField:hello">>,<<"KKKRF">>],
   ?assertEqual(to_fields(test_bin_fields()), Res).

bitmask_source_test() ->
   ?assertEqual(eval_bitarray(5, ?SOURCE_BITMASK_VAL), [<<"goto_exit">>, <<"recirculate">>]).

bitmask_target_test() ->
   ?assertEqual(eval_bitarray(33, ?TARGET_BITMASK_VAL),[<<"not_scanned">>, <<"recirculate">>]).

-endif.
