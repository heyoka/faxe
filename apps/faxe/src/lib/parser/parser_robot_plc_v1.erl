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
-export([parse/1, test/0, pos1_test/0]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(BOOL, 1/integer-unsigned).
-define(INT, 16/integer).
-define(REAL, 32/float).
-define(DINT, 32/integer).

-define(X, <<"X">>).
-define(Y, <<"Y">>).
-define(Z, <<"Z">>).
-define(YAW, <<"Yaw">>).
-define(PITCH, <<"Pitch">>).
-define(SUCC1, <<"Suction1">>).
-define(SUCC2, <<"Suction2">>).
-define(SUCC3, <<"Suction3">>).

%% test the macro
test() ->
   <<Flag:?BOOL>> = <<0:1/integer>>,
   Flag.

parse(BinData) ->
   Acc = #{
      <<"DateTimeUTC">> => 0,
      %% data
      <<"ToolPoint">> => #{},
      ?X => #{},
      ?Y => #{},
      ?Z => #{},
      ?YAW => #{},
      ?PITCH => #{},
      ?SUCC1 => #{},
      ?SUCC2 => #{},
      ?SUCC3 => #{}},
   jsx:encode(pos1(BinData, Acc)).

%% we parse the data in chunks ...
%% first 20 bytes
pos1(
    <<PosX:?REAL,
       PosY:?REAL,
       PosZ:?REAL,
       PosYaw:?REAL,
       PosPitch:?REAL, Second/binary>>, Acc) ->

   NewAcc =
      Acc#{<<"ToolPoint">> =>
      #{?X => PosX, ?Y => PosY, ?Z => PosZ, ?YAW => PosYaw, ?PITCH => PosPitch}},
   pos2(Second, NewAcc).

%% second 20 bytes
%% Drive Position
pos2(
    <<PosA1:?REAL,
       PosA2:?REAL,
       PosA3:?REAL,
       PosA4:?REAL,
       PosA5:?REAL, Third/binary>>, Acc=#{}) ->
   NewAcc = set_axis_val(<<"DrivePosition">>, [PosA1, PosA2, PosA3, PosA4, PosA5], Acc),
   third(Third, NewAcc).

%% speed and suction
%% 12 bytes
third(
    <<Speed:?REAL,
       Accel:?REAL,
       Succ1State:?INT,
       Succ2State:?INT,
       Succ3State: ?INT, Fourth/binary>>, Acc) ->
   NewAcc0 = set_succ_value(<<"VacuumState">>, [Succ1State, Succ2State, Succ3State], Acc),
   NewAcc = NewAcc0#{<<"Velocity">> => Speed, <<"Acceleration">> => Accel},
   fourth(Fourth, NewAcc).

%% 1 byte
fourth(
    <<SensStateSucc1:?BOOL,
       SensStateSucc2:?BOOL,
       SensStateSucc3:?BOOL,
       UltraSoundSenseState:?BOOL, 0:4, Fifth/binary>>, Acc) ->
   NewAcc = set_succ_value(<<"ActiveState">>, [SensStateSucc1, SensStateSucc2, SensStateSucc3], Acc),
   fifth(Fifth, NewAcc#{<<"UltraSoundSensorState">> => UltraSoundSenseState}).

%% 12 bytes
fifth(<<SenseSucc1:?REAL, SenseSucc2:?REAL, SenseSucc3:?REAL, Sixth/binary>>, Acc=#{}) ->
   NewAcc = set_succ_value(<<"Pressure">>, [SenseSucc1, SenseSucc2, SenseSucc3], Acc),
   sixth(Sixth, NewAcc).

%% 16 byte
sixth(
    <<WeightScalePick1:?REAL,
       WeightScaleDrop1:?REAL,
       WeightScalePick2:?REAL,
       WeightScaleDrop2:?REAL, Seventh/binary>>, Acc=#{}) ->

   seventh(Seventh, Acc#{
      <<"WeightScalePick1">> => WeightScalePick1, <<"WeightScalePick2">> => WeightScalePick2,
      <<"WeightScaleDrop1">> => WeightScaleDrop1, <<"WeightScaleDrop2">> => WeightScaleDrop2}).

%% 24 bytes
seventh(
    <<ErrorID:?DINT, %% ??? RobotErrorState
       ErrorID_X:?DINT,
       ErrorID_Y:?DINT,
       ErrorID_Z:?DINT,
       ErrorID_Yaw:?DINT,
       ErrorID_Pitch:?DINT, Eigth/binary>>, Acc=#{}) ->

   NewAcc = set_axis_val(<<"ErrorCode">>,
      [ErrorID_X, ErrorID_Y, ErrorID_Z, ErrorID_Yaw, ErrorID_Pitch], Acc),
   eighth(Eigth, NewAcc#{<<"ErrorCode">> => ErrorID}).

%% 1 byte
eighth(
    <<XAxisRefState:?BOOL,
       YAxisRefState:?BOOL,
       ZAxisRefState:?BOOL,
       YawAxisRefState:?BOOL,
       PitchAxisRefState:?BOOL, 0:3, Motor/binary>>, Acc) ->
   NewAcc = set_axis_val(<<"ReferencedState">>,
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
   {#{<<"LoadLimitIntegral">> => Motorl2t, <<"Torque">> => MotorTorque, <<"Temperature">> => MotorTemp,
      <<"Current">> => MotorA, <<"LagError">> => MotorErr},
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
         <<"ReferenceButtonState">> => RefButtonStateX, <<"Limit1State">> => Limit1StateX,
         <<"Limit2State">> => Limit2StateX},
      ?Y => MapY#{
         <<"ReferenceButtonState">> => RefButtonStateY, <<"Limit1State">> => Limit1StateY,
         <<"Limit2State">> => Limit2StateY},
      ?Z => MapZ#{
         <<"ReferenceButtonState">> => RefButtonStateZ, <<"Limit1State">> => Limit1StateZ,
         <<"Limit2State">> => Limit2StateZ
      }},
   tenth(Fifteenth,
      NewAcc#{<<"RobotErrorState">> => RobotError, <<"DrivesActiveState">> => MotorActive,
         <<"PlcConnectionState">> => PLCConnActive, <<"ConveyorPositionStateDrop1">> => RobotFreeDP1,
         <<"ConveyorPositionStateDrop2">> => RobotFreeDP2,
         <<"ConveyorPositionStatePick1">> => RobotFreePP1,
         <<"ConveyorPositionStatePick2">> => RobotFreePP2}).

%% 6 bytes
tenth(<<CameraConnectionActive:?INT, RobotMode:?INT, CamMode:?INT>>, Acc=#{}) ->
   Acc#{<<"CameraConnectionState">> => CameraConnectionActive, <<"OperatingMode">> => RobotMode,
      <<"CameraOpratingMode">> => CamMode}.

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


%%%%%%%%%%%%%%%%%% end %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

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
   lager:warning("Time needed: ~p~nResult: ~p~nJSON: ~s~nWust: ~p~nByteSize:~p",
      [T, Res, jsx:encode(Res), Wurst, byte_size(Wurst)]).

make_reals(Num) ->
   L = part_list(Num, fun(I) -> faxe_lambda_lib:random_real(I) end),
   << <<X:32/float>> || X <- L >>.
make_dints(Num) ->
   L = part_list(Num, fun(I) -> faxe_lambda_lib:random(I) end),
   << <<X:32/integer>> || X <- L >>.
part_list(Num, ProdFun) ->
   [ProdFun(40+R) || R <- lists:seq(1,Num)].


-ifdef(TEST).

-endif.
