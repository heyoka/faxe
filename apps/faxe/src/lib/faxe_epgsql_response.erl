%%%-------------------------------------------------------------------
%%% @author heyoka
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 07. Nov 2021 18:52
%%%-------------------------------------------------------------------
-module(faxe_epgsql_response).
-author("heyoka").

-include("faxe.hrl").
-include("faxe_epgsql_response.hrl").
%% API
-export([new/3, handle/2]).

new(TimeField, ResponseType, PointRootObject) ->
  #faxe_epgsql_response{
    time_field = TimeField,
    response_type = ResponseType,
    point_root_object = PointRootObject
  }.

handle({ok, Count}, _ResponseDef) ->
  lager:notice("count response: ~p",[Count]),
  ok;
handle({ok, Count, Columns, Rows}, _ResponseDef) ->
  lager:notice("count, col, rows response: ~p",[{Count, Columns, Rows}]),
  ok;
handle({ok, Columns, Rows}=_R, ResponseDef = #faxe_epgsql_response{}) ->
%%  lager:info("IN: ~p",[length(Rows)]),
  ColumnNames = columns(Columns, []),
%%   lager:notice("result ROWS: ~p",[Rows]),
  Batch = handle_result(ColumnNames, Rows, ResponseDef),
%%  lager:info("OUT: ~p",[Batch]),
  {ok, Batch};
handle(Other, _ResponseDef) ->
  Other.


columns([], ColumnNames) ->
  lists:reverse(ColumnNames);
columns([{column, Name, _Type, _, _, _, _, _, _}|RestC], ColumnNames) ->
  columns(RestC, [Name|ColumnNames]).

handle_result(Columns, Rows, ResponseDef = #faxe_epgsql_response{response_type = batch}) ->
  to_flowdata(Columns, Rows, ResponseDef);
handle_result(Columns, Rows, ResponseDef = #faxe_epgsql_response{response_type = point, point_root_object = Root}) ->
  Batch = to_flowdata(Columns, Rows, ResponseDef),
  FieldsList = [Fields || #data_point{fields = Fields} <- Batch#data_batch.points],
  #data_point{ts = faxe_time:now(), fields = #{Root => FieldsList}}.


to_flowdata(Columns, ValueRows, ResponseDef = #faxe_epgsql_response{default_timestamp = QueryStart}) ->
  VRows = [tuple_to_list(VRow) || VRow <- ValueRows],
  to_flowdata(Columns, lists:reverse(VRows), #data_batch{start = QueryStart}, ResponseDef).

to_flowdata(_Columns, [], Batch=#data_batch{}, _ResponseDef) ->
  Batch;
to_flowdata(Columns, [ValRow|Values], Batch=#data_batch{points = Points},
    R = #faxe_epgsql_response{time_field = TimeField, default_timestamp = DefaultTs}) ->
  Point = row_to_datapoint(Columns, ValRow, #data_point{ts = DefaultTs}, TimeField),
  to_flowdata(Columns, Values, Batch#data_batch{points = [Point|Points]}, R).

row_to_datapoint([], [], Point, _TimeField) ->
  Point;
%% ignore rows where ts is 'null'
row_to_datapoint([TimeField|Columns], [null|Row], Point, TimeField) ->
  row_to_datapoint(Columns, Row, Point, TimeField);
row_to_datapoint([TimeField|Columns], [Ts|Row], Point, TimeField) ->
%%  lager:info("got ts: ~p",[Ts]),
  P = Point#data_point{ts = decode_ts(Ts)},
  row_to_datapoint(Columns, Row, P, TimeField);
row_to_datapoint([C|Columns], [Val|Row], Point, TimeField) ->
  P = flowdata:set_field(Point, C, Val),
  row_to_datapoint(Columns, Row, P, TimeField).

%% ms timestamp in bin
decode_ts(<<Ts:64>>) ->
  Ts;
%% unix ts in sec as integer
decode_ts(Ts) when is_integer(Ts) ->
  Ts*1000;
%% some kind of erlang now format (or with Second.Millisecond)
decode_ts({_Date, {_Hour, _Minute, _SecondFrac}} = DateTime) ->
  faxe_time:to_ms(DateTime);
%% datetime string, we assume it is in iso8601 format
decode_ts(DtString) when is_binary(DtString) ->
  time_format:iso8601_to_ms(DtString).

-ifdef(TEST).
first_test() ->
  DataIn =
    {ok,
      [
        {column,<<"dt_ts">>,varchar,1043,-1,-1,1,0,0},
        {column,<<"id">>,varchar,1043,-1,-1,1,0,2},
        {column,<<"energy">>,json,114,-1,-1,1,0,658}],
      [
        {<<"2021-11-08T00:01:30.000000Z">>,<<"data/556/reg1/em/energy/v1">>,
          #{<<"ActiveEnergyDelvd">> => 19495.668,<<"ActiveEnergyRcvd">> => 11.375,<<"ApparentEnergyDelvd">> => 33069.973,
            <<"ApparentEnergyRcvd">> => 53.269,<<"MaximalCurrentValue">> => 4.088959,<<"ReactiveEnergyDelvd">> => 10.527,
            <<"ReactiveEnergyRcvd">> => 26404.852}},
        {<<"2021-11-08T00:01:00.000000Z">>,<<"data/556/reg1/em/energy/v1">>,
          #{<<"ActiveEnergyDelvd">> => 19495.654,<<"ActiveEnergyRcvd">> => 11.375,<<"ApparentEnergyDelvd">> => 33069.95,
            <<"ApparentEnergyRcvd">> => 53.269,<<"MaximalCurrentValue">> => 4.0775533,<<"ReactiveEnergyDelvd">> => 10.527,
            <<"ReactiveEnergyRcvd">> => 26404.832}}
      ]
    },

  DataOut =
    {ok,{data_batch,undefined,
      [{data_point,1636329690000,
        #{<<"energy">> =>
        #{<<"ActiveEnergyDelvd">> => 19495.668,
          <<"ActiveEnergyRcvd">> => 11.375,
          <<"ApparentEnergyDelvd">> => 33069.973,
          <<"ApparentEnergyRcvd">> => 53.269,
          <<"MaximalCurrentValue">> => 4.088959,
          <<"ReactiveEnergyDelvd">> => 10.527,
          <<"ReactiveEnergyRcvd">> => 26404.852},
          <<"id">> => <<"data/556/reg1/em/energy/v1">>},
        #{},undefined,<<>>},
        {data_point,1636329660000,
          #{<<"energy">> =>
          #{<<"ActiveEnergyDelvd">> => 19495.654,
            <<"ActiveEnergyRcvd">> => 11.375,
            <<"ApparentEnergyDelvd">> => 33069.95,
            <<"ApparentEnergyRcvd">> => 53.269,
            <<"MaximalCurrentValue">> => 4.0775533,
            <<"ReactiveEnergyDelvd">> => 10.527,
            <<"ReactiveEnergyRcvd">> => 26404.832},
            <<"id">> => <<"data/556/reg1/em/energy/v1">>},
          #{},undefined,<<>>}],
      undefined,undefined,undefined}},

  Def = #faxe_epgsql_response{time_field = <<"dt_ts">>, response_type = batch},
  ?assertEqual(DataOut, handle(DataIn, Def)).


-endif.