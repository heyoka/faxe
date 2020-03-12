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

-behavior(binary_msg_parser).

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
   Out1 = Out0#{<<"alarm_state">> => AlarmStateMap},
   State = robot_state(OpState, OpMode, Avail, AlarmState),
   Out = Out1#{<<"robot_state">> => State},
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

%% overall robot state derived from the 4 parsed vals
%% operating_state, operating_mode, order_state(avail), alarm_state

%%robot_state(OpState, OpMode, Avail, ErrState) -> ok;
robot_state(_OpState, _OpMode, _Avail, <<"ERR">>) -> <<"ERROR">>; %% Error, any other state
robot_state(_OpState, <<"MANU">>, _Avail, _Err) -> <<"OFF">>; %% Manuell
robot_state(<<"OFF">>, _OpMode, _Avail, _Err) -> <<"OFF">>; %% Off at OpState
robot_state(<<"ON">>, <<"AUTO">>, <<"NRDY">>, _Err) -> <<"BUSY">>;
robot_state(<<"ON">>, <<"AUTO">>, <<"RDY">>, _Err) -> <<"IDLE">>.

%%%%%%%%%%%%%%%%%% end %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

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
            #{<<"id">> => 1,<<"name">> => <<"NRDY">>},
         <<"robot_state">> => <<"ERROR">>}
      }
      ,parse(Data)
   ).

basic_2_test() ->

   Data = <<"ROB1;MFS1;4272;RMST;ON;AUTO;NOAL;NRDY;[]\r\n">>,
   ?assertEqual(
      {<<"05.010">>,1,
         #{<<"alarm_state">> =>
         #{<<"id">> => 0,<<"name">> => <<"NOAL">>},
            <<"operating_mode">> =>
            #{<<"id">> => 1,<<"name">> => <<"AUTO">>},
            <<"operating_state">> =>
            #{<<"id">> => 0,<<"name">> => <<"ON">>},
            <<"order_state">> =>
            #{<<"id">> => 1,<<"name">> => <<"NRDY">>},
            <<"robot_state">> => <<"BUSY">>}}
      ,parse(Data)
   ).
-endif.

