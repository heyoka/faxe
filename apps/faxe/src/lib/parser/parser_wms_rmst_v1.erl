%%%-------------------------------------------------------------------
%%% @author heyoka
%%% @copyright (C) 2019, <COMPANY>
%%% @doc
%%% parser for the RMST message from the WMS interface
%%% @end
%%% Created : 09. Dec 2019 13:53
%%%-------------------------------------------------------------------
-module(parser_wms_rmst_v1).
-author("heyoka").

-behavior(tcp_msg_parser).

%% API
-export([parse/1]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(PARSER_VERSION, 1).
-define(TGW_DATAFORMAT, <<"05.010">>).

-define(FIELD_DELIMITER, [<<";">>, <<"\r\n">>]).
-define(ERR_DELIMITER, [<<",">>, <<"]">>, <<" ">>]).

parse(BinData) ->
   %% split lines
   FieldsAll = binary:split(BinData, ?FIELD_DELIMITER, [global, trim_all]),
   [_FromRob,_To, _RNr, <<"RMST">>, OpState, OpMode, AlarmState, Avail, ErrorStates] = FieldsAll,
   Out0 = #{
      <<"operating_state">> => #{<<"id">> => enum_opstate(OpState), <<"name">> => OpState},
      <<"operating_mode">> => #{<<"id">> => enum_opmode(OpMode), <<"name">> => OpMode},
      <<"order_state">> => #{<<"id">> => enum_orderstate(Avail), <<"name">> => Avail}
   },
   AlarmStateMap0 = #{<<"id">> => enum_alarmstate(AlarmState), <<"name">> => AlarmState},
   AlarmStateMap =
   case AlarmState of
      <<"ERR">> ->
         Errors = parse_errors(ErrorStates),
         AlarmStateMap0#{<<"errors">> => Errors};
      _ -> AlarmStateMap0
   end,
   Out = Out0#{<<"alarm_state">> => AlarmStateMap},
   lager:notice("the fields: ~p" ,[Out]),
   {?TGW_DATAFORMAT, ?PARSER_VERSION, Out}.

enum_opstate(<<"ON">>) -> 0;
enum_opstate(<<"OFF">>) -> 1.
enum_opmode(<<"NOMO">>) -> 0;
enum_opmode(<<"AUTO">>) -> 1;
enum_opmode(<<"MANU">>) -> 2.
enum_alarmstate(<<"NOAL">>) -> 0;
enum_alarmstate(<<"ERR">>) -> 1.
enum_orderstate(<<"RDY">>) -> 0; %% availablility
enum_orderstate(<<"NRDY">>) -> 1;
enum_orderstate(<<"OFF">>) -> 2.

parse_errors(<<"[]">>) -> [];
parse_errors(<<"[", Rest/binary>>) ->
   binary:split(Rest, ?ERR_DELIMITER, [global, trim_all]).

%%%%%%%%%%%%%%%%%% end %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-ifdef(TEST).
basic_test() ->

   Data = <<"ROB1;MFS1;4278;RMST;ON;AUTO;ERR;NRDY;[SLCI, SESA]\r\n">>,
   ?assertEqual(
      {<<"05.010">>,1,
         #{<<"alarm_state">> =>
         #{<<"errors">> => [<<"SLCI">>,<<"SESA">>],
            <<"id">> => 1,<<"name">> => <<"ERR">>},
            <<"operating_mode">> =>
            #{<<"id">> => 1,<<"name">> => <<"AUTO">>},
            <<"operating_state">> =>
            #{<<"id">> => 0,<<"name">> => <<"ON">>},
            <<"order_state">> =>
            #{<<"id">> => 1,<<"name">> => <<"NRDY">>}}}
      ,parse(Data)
   ).
-endif.

