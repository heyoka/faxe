%%%-------------------------------------------------------------------
%%% @author heyoka
%%% @copyright (C) 2019, <COMPANY>
%%% @doc
%%% parser for the robot_plc data v1
%%% siemens plc cpus are big-endian (as is erlang's default)
%%%
%%% 216 bytes in total
%%% @end
%%% Created : 01. Aug 2019 16:53
%%%-------------------------------------------------------------------
-module(parser_robot_plc_v1).
-author("heyoka").

-behavior(tcp_msg_parser).

%% API
-export([parse/1]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(PARSER_VERSION, 1).
-define(TGW_DATAFORMAT, <<"03.001">>).

-define(BOOL, 1/integer-unsigned).
-define(INT, 16/integer).
-define(REAL, 32/float).
-define(DINT, 32/integer).

-define(X, <<"x">>).
-define(Y, <<"y">>).
-define(Z, <<"z">>).
-define(YAW, <<"yaw">>).
-define(PITCH, <<"pitch">>).
-define(SUCC1, <<"suc1">>).
-define(SUCC2, <<"suc2">>).
-define(SUCC3, <<"suc3">>).

parse(BinData) ->
   Acc = #{
      %% data
      <<"tool">> => #{},
      ?X => #{},
      ?Y => #{},
      ?Z => #{},
      ?YAW => #{},
      ?PITCH => #{},
      ?SUCC1 => #{},
      ?SUCC2 => #{},
      ?SUCC3 => #{}},
   {?TGW_DATAFORMAT, ?PARSER_VERSION, pos1(BinData, Acc)}.
%%   jsx:encode(pos1(BinData, Acc)).

%% we parse the data in chunks ...
%% first 20 bytes
pos1(
    <<PosX:?REAL,
       PosY:?REAL,
       PosZ:?REAL,
       PosYaw:?REAL,
       PosPitch:?REAL, Second/binary>>, Acc) ->

   NewAcc =
      Acc#{<<"tool">> =>
      #{?X => round(PosX), ?Y => round(PosY), ?Z => round(PosZ),
         ?YAW => round(PosYaw), ?PITCH => round(PosPitch)}},
   pos2(Second, NewAcc).

%% second 20 bytes
%% Drive Position
pos2(
    <<PosA1:?REAL,
       PosA2:?REAL,
       PosA3:?REAL,
       PosA4:?REAL,
       PosA5:?REAL, Third/binary>>, Acc=#{}) ->
   NewAcc = set_axis_val(<<"pos">>,
      [round(PosA1), round(PosA2), round(PosA3), round(PosA4), round(PosA5)], Acc),
   third(Third, NewAcc).

%% speed and suction
%% 12 bytes
third(
    <<Speed:?REAL,
       Accel:?REAL,
       Succ1State:?INT,
       Succ2State:?INT,
       Succ3State: ?INT, Fourth/binary>>, Acc) ->
   NewAcc0 = set_succ_value(<<"stat">>, [Succ1State, Succ2State, Succ3State], Acc),
   NewAcc = NewAcc0#{<<"vel">> => Speed, <<"acc">> => Accel},
   fourth(Fourth, NewAcc).

%% 1 byte
fourth(
    <<SensStateSucc1:?BOOL,
       SensStateSucc2:?BOOL,
       SensStateSucc3:?BOOL,
       UltraSoundSenseState:?BOOL, _Bit4:1, _Bit5:1, _Bit6:1, _Bit7:1, Fifth/binary>>, Acc) ->
%%   lager:warning("Succ1, Succ2, Succ3, Ultrasound, B4, B5, B6, B7: ~p",[
%%      {SensStateSucc1, SensStateSucc2, SensStateSucc3, UltraSoundSenseState, Bit4, Bit5, Bit6, Bit7}]),
   NewAcc = set_succ_value(<<"act">>, [SensStateSucc1, SensStateSucc2, SensStateSucc3], Acc),
   fifth(Fifth, NewAcc#{<<"usnd">> => UltraSoundSenseState}).

%% 12 bytes
fifth(<<SenseSucc1:?REAL, SenseSucc2:?REAL, SenseSucc3:?REAL, Sixth/binary>>, Acc=#{}) ->
   NewAcc = set_succ_value(<<"press">>, [SenseSucc1, SenseSucc2, SenseSucc3], Acc),
   sixth(Sixth, NewAcc).

%% 16 byte
sixth(
    <<WeightScalePick1:?REAL,
       WeightScaleDrop1:?REAL,
       WeightScalePick2:?REAL,
       WeightScaleDrop2:?REAL, Seventh/binary>>, Acc=#{}) ->

   seventh(Seventh, Acc#{
      <<"wpp1">> => to_gramms(WeightScalePick1), <<"wpp2">> => to_gramms(WeightScalePick2),
      <<"wdp1">> => to_gramms(WeightScaleDrop1), <<"wdp2">> => to_gramms(WeightScaleDrop2)}).

%% 24 bytes
seventh(
    <<ErrorID:?DINT, %% ??? RobotErrorState
       ErrorID_X:?DINT,
       ErrorID_Y:?DINT,
       ErrorID_Z:?DINT,
       ErrorID_Yaw:?DINT,
       ErrorID_Pitch:?DINT, Eigth/binary>>, Acc=#{}) ->

   NewAcc = set_axis_val(<<"err">>,
      [ErrorID_X, ErrorID_Y, ErrorID_Z, ErrorID_Yaw, ErrorID_Pitch], Acc),
   eighth(Eigth, NewAcc#{<<"errcode">> => ErrorID}).

%% 1 byte
eighth(
    <<XAxisRefState:?BOOL,
       YAxisRefState:?BOOL,
       ZAxisRefState:?BOOL,
       YawAxisRefState:?BOOL,
       PitchAxisRefState:?BOOL, _:1, _:1, _:1, Motor/binary>>, Acc) ->
   NewAcc = set_axis_val(<<"refs">>,
      [XAxisRefState, YAxisRefState, ZAxisRefState, YawAxisRefState, PitchAxisRefState], Acc),
   motor(Motor, NewAcc).

%% 5*20 bytes = 100 bytes
motor(MotorX,
    Acc=#{?X := MapX, ?Y := MapY, ?Z := MapZ, ?YAW := MapYaw, ?PITCH := MapPitch}) ->
   {AccX, MotorY} = parse_motor(MotorX),
   NewMapX = maps:merge(MapX,AccX),
   {AccY, MotorZ} = parse_motor(MotorY),
   NewMapY = maps:merge(MapY, AccY),
   {AccZ, MotorYaw} = parse_motor(MotorZ),
   NewMapZ = maps:merge(MapZ, AccZ),
   {AccYaw, MotorPitch} = parse_motor(MotorYaw),
   NewMapYaw = maps:merge(MapYaw, AccYaw),
   {AccPitch, Rest} = parse_motor(MotorPitch),
   NewMapPitch = maps:merge(MapPitch, AccPitch),
   NewAcc =
      Acc#{?X := NewMapX, ?Y := NewMapY, ?Z := NewMapZ, ?YAW := NewMapYaw, ?PITCH := NewMapPitch},
   ninth(Rest, NewAcc).

%% (20 bytes)
parse_motor(<<Motorl2t:?REAL, MotorTorque:?REAL, MotorTemp:?REAL, MotorA:?REAL, MotorErr:?REAL, Rest/binary>>) ->
   {#{<<"i2t">> => Motorl2t, <<"tor">> => MotorTorque, <<"temp">> => MotorTemp,
      <<"cur">> => MotorA, <<"lag">> => MotorErr},
      Rest}.

%% 2 byte
ninth(
    <<RefButtonStateX:?BOOL,
       RefButtonStateY:?BOOL,
       RefButtonStateZ:?BOOL,
       Limit1StateX:?BOOL,
       Limit2StateX:?BOOL,
       Limit1StateY:?BOOL,
       Limit2StateY:?BOOL,
       Limit1StateZ:?BOOL,
       %% 1 Byte full
       Limit2StateZ:?BOOL,
       RobotError:?BOOL,
       MotorActive:?BOOL,
       PLCConnActive:?BOOL,
       RobotFreeDP1:?BOOL,
       RobotFreeDP2:?BOOL,
       RobotFreePP1:?BOOL,
       RobotFreePP2:?BOOL,
       %% 1 Byte full
       Fifteenth/binary>>, Acc=#{?X := MapX, ?Y := MapY, ?Z := MapZ}) ->
   NewAcc = Acc#{
      ?X => MapX#{
         <<"refb">> => RefButtonStateX, <<"lim1">> => Limit1StateX,
         <<"lim2">> => Limit2StateX},
      ?Y => MapY#{
         <<"refb">> => RefButtonStateY, <<"lim1">> => Limit1StateY,
         <<"lim2">> => Limit2StateY},
      ?Z => MapZ#{
         <<"refb">> => RefButtonStateZ, <<"lim1">> => Limit1StateZ,
         <<"lim2">> => Limit2StateZ
      }},
   tenth(Fifteenth,
      NewAcc#{<<"rerr">> => RobotError, <<"dact">> => MotorActive,
         <<"conn">> => PLCConnActive, <<"sdp1">> => RobotFreeDP1,
         <<"sdp2">> => RobotFreeDP2,
         <<"spp1">> => RobotFreePP1,
         <<"spp2">> => RobotFreePP2}).

%% 6 bytes
tenth(<<CameraConnectionActive:?INT, RobotMode:?INT, CamMode:?INT>>, Acc=#{}) ->
   Acc#{<<"camstat">> => CameraConnectionActive, <<"opmode">> => RobotMode,
      <<"camopm">> => CamMode}.

set_axis_val(Name, Values,
    Acc=#{?X := MapX, ?Y := MapY, ?Z := MapZ, ?YAW := MapYaw, ?PITCH := MapPitch}) ->
   Acc#{
      ?X => MapX#{Name => lists:nth(1, Values)}, ?Y => MapY#{Name => lists:nth(2, Values)},
      ?Z => MapZ#{Name => lists:nth(3, Values)}, ?YAW => MapYaw#{Name => lists:nth(4, Values)},
      ?PITCH => MapPitch#{Name => lists:nth(5, Values)}}.

set_succ_value(Name, Values, Acc = #{?SUCC1 := Succ1, ?SUCC2 := Succ2, ?SUCC3 := Succ3}) ->
   Acc#{
      ?SUCC1 => Succ1#{Name => lists:nth(1, Values)},
      ?SUCC2 => Succ2#{Name => lists:nth(2, Values)},
      ?SUCC3 => Succ3#{Name => lists:nth(3, Values)}}.

to_gramms(V) -> round(V*1000).
%%%%%%%%%%%%%%%%%% end %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-ifdef(TEST).

%% test the macro
test() ->
   <<Flag:?BOOL>> = <<0:1/integer>>,
   Flag.

pos1_test() ->

   Pos1 = make_reals(5),
   Pos2 = make_reals(5),
   Pos21 = make_reals(2), Pos22 = <<252:16/integer, 2323:16/integer, 6786:16/integer>>,
   Pos3 = <<0:1, 1:1, 1:1, 0:1, 0:4>>,
   Pos4 = make_reals(3),
   Pos5 = make_reals(4),
   Pos6 = make_dints(6),
   Pos7 = <<0:1, 0:1, 1:1, 1:1, 0:1, 0:3>>,
   Motor = make_reals(25),
   Bools = <<0:1, 0:1, 1:1, 1:1, 0:1, 0:1, 1:1, 1:1, 0:1, 0:1, 1:1, 0:1, 1:1, 1:1, 0:1, 0:1>>,
   Last = <<2323:?INT, 2329:?INT, 99877:?INT>>,
   Wurst = iolist_to_binary([Pos1, Pos2, Pos21, Pos22, Pos3, Pos4, Pos5, Pos6, Pos7, Motor, Bools, Last]),
   {T, Res} = timer:tc(?MODULE, parse, [Wurst]),
   lager:warning("Time needed: ~p~nResult: ~p~nJSON: ~s~nWurst: ~p~nByteSize:~p",
      [T, Res, jsx:encode(Res), Wurst, byte_size(Wurst)]).

make_reals(Num) ->
   L = part_list(Num, fun(I) -> faxe_lambda_lib:random_real(I) end),
   << <<X:32/float>> || X <- L >>.
make_dints(Num) ->
   L = part_list(Num, fun(I) -> faxe_lambda_lib:random(I) end),
   << <<X:32/integer>> || X <- L >>.
part_list(Num, ProdFun) ->
   [ProdFun(40+R) || R <- lists:seq(1,Num)].

-endif.
