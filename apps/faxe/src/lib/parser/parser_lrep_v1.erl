%%%-------------------------------------------------------------------
%%% @author heyoka
%%% @copyright (C) 2020
%%% @doc
%%% parser for the MFS Interface, 4.3.1	Location Report (LREP)
%%% @end
%%% Created : 12. Mai 2020 16:53
%%%-------------------------------------------------------------------
-module(parser_lrep_v1).
-author("heyoka").

-behavior(binary_msg_parser).

%% API
-export([parse/1]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(PARSER_VERSION, 1).
-define(TGW_DATAFORMAT, <<"12.001">>).

%%   Identifier	Description
%%   NDIR	Request for destination (no destination available)
%%   EDIR	Transport to destination is not possible
%%   NLCD	Need LC data to this LC
%%   NLDD	Need LC data and destination to this LC
%%   ELCD	Error Load Carrier Data
%%   ACKN	An acknowledge is required
%%   NLCA	Need LC action
%%   MISS	Destination missed (unknown or unspecified reason)
%%   TRAC	Destination missed – tracking error
%%   FULL	Destination missed – destination full
%%   NAUT	Destination missed – destination is not in automatic mode
%%   ALST	Destination missed – acknowledge list full
%%   MEER	Destination missed – destination with mechatronic error
%%   NOTA	no target available (Pick Center ONE TS)
%%   ETRC	Error at Tracking (Pick Center ONE TS)


-define(FIELD_DELIMITER, <<";">>).
-define(LIST_DELIMITER, <<",">>).
-define(VALUE_DELIMITER, <<":">>).

parse(BinData) ->
   %% split fields
   Fields = to_fields(BinData),
   Res = convert(Fields),
   {?TGW_DATAFORMAT, ?PARSER_VERSION, Res}.

to_fields(LineData) ->
   binary:split(LineData, ?FIELD_DELIMITER, [global]).

convert([
   SenderId, RecvId, TelId, TelType = <<"LREP">>,  %% Header
   McNo, LcId, ReadStatus, Address, Status, LoadCarrierDataList %% LREP telegram
]) ->
   LcDt0 = binary:replace(LoadCarrierDataList, [<<"[">>, <<"]">>, <<"(">>, <<")">>], <<>>, [global]),
   LcDt1 = binary:split(LcDt0, ?LIST_DELIMITER, [global, trim_all]),
   LcDt2 = [binary:split(E, ?VALUE_DELIMITER, [global, trim_all]) || E <- LcDt1],
   LcDt3 = [#{K => lcdata_value(V)} || [K, V] <- LcDt2],
   #{
      <<"SndId">> => SenderId,
      <<"RcvId">> => RecvId,
      <<"TelId">> => binary_to_integer(TelId),
      <<"TelTyp">> => TelType,
      <<"MCNo">> => binary_to_integer(McNo),
      <<"LcId">> => remove_quotes(LcId),
      <<"RStat">> => ReadStatus,
      <<"Addr">> => Address,
      <<"Stat">> => Status,
      <<"LcDt">> => LcDt3
   }.

lcdata_value(<<"\"", _R/binary>> = Bin) ->
   remove_quotes(Bin);
lcdata_value(Bin) ->
   binary_to_integer(Bin).

remove_quotes(Bin) ->
   binary:replace(Bin, <<"\"">>, <<>>, [global]).


%%%%%%%%%%%%%%%%%% end %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-ifdef(TEST).
%% test
def() ->
   <<"F001;;16;LREP;16;\"NOREAD011111800150\";N;F01LP_SC1;NDIR;[(WDCL:1),(HGCL:2),(WEIG:4200)]">>.
def1() ->
   <<"F001;;2133;LREP;944;\"545860\";N;F1IN1;;[(LID1:\"542480\"),(LID2:\"681580\")]">>.
def2() ->
   <<"F001;;16;LREP;23;\"NOREAD011111800150\";N;F01LP_SC1;NDIR;[]">>.

res() ->
   #{<<"Addr">> => <<"F01LP_SC1">>,
      <<"LcDt">> => [#{<<"WDCL">> => 1},#{<<"HGCL">> => 2},#{<<"WEIG">> => 4200}],
      <<"LcId">> => <<"NOREAD011111800150">>,
      <<"MCNo">> => 16,
      <<"RStat">> => <<"N">>,
      <<"RcvId">> => <<>>,
      <<"SndId">> => <<"F001">>,
      <<"Stat">> => <<"NDIR">>,
      <<"TelId">> => 16,
      <<"TelTyp">> => <<"LREP">>}.

res1() ->
   #{<<"Addr">> => <<"F1IN1">>,
      <<"LcDt">> =>
      [#{<<"LID1">> => <<"542480">>}, #{<<"LID2">> => <<"681580">>}],
      <<"LcId">> => <<"545860">>,<<"MCNo">> => 944,
      <<"RStat">> => <<"N">>,<<"RcvId">> => <<>>,
      <<"SndId">> => <<"F001">>,<<"Stat">> => <<>>,
      <<"TelId">> => 2133,
      <<"TelTyp">> => <<"LREP">>}.

res2() ->
   #{<<"Addr">> => <<"F01LP_SC1">>,
      <<"LcDt">> => [],
      <<"LcId">> => <<"NOREAD011111800150">>,
      <<"MCNo">> => 23,
      <<"RStat">> => <<"N">>,
      <<"RcvId">> => <<>>,
      <<"SndId">> => <<"F001">>,
      <<"Stat">> => <<"NDIR">>,
      <<"TelId">> => 16,
      <<"TelTyp">> => <<"LREP">>}.

basic_1_test() ->
   ?assertEqual({?TGW_DATAFORMAT, ?PARSER_VERSION, res()}, parse(def())).

basic_2_test() ->
   ?assertEqual({?TGW_DATAFORMAT, ?PARSER_VERSION, res1()}, parse(def1())).

basic_3_test() ->
   ?assertEqual({?TGW_DATAFORMAT, ?PARSER_VERSION, res2()}, parse(def2())).

-endif.
