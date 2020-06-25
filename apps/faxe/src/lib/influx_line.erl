%%%-------------------------------------------------------------------
%%% @author heyoka
%%% @copyright (C) 2020, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 23. Jun 2020 15:04
%%%-------------------------------------------------------------------
-module(influx_line).
-author("heyoka").


%% API:
-export([
  encode/1,
  encode/2
]).

-define(
encode_timestamp(X, Y, Z),
  (((X * 1000000) + Y) * 1000000 + Z) * 1000
).

%% -----------------------------------------------------------------------------
%% API:

encode(Data) ->
  encode(Data, #{}).


encode(Data, Opts) when erlang:is_map(Opts) ->
  Result = encode(
    Data,
    maps:get(encode_integer, Opts, false),
    maps:get(set_timestamp, Opts, false)
  ),
%%  lager:notice("Result iolist: ~p",[Result]),
  case maps:get(return_type, Opts, iolist) of
    iolist ->
      Result;
    unsafe_string ->
      lists:flatten(Result);
    string ->
      erlang:binary_to_list(erlang:iolist_to_binary(Result));
    _ -> % binary
      erlang:iolist_to_binary(Result)
  end.

%% -----------------------------------------------------------------------------
%% Internals:

encode(
    {Key, Fields, Timestamp},
    EncodeIntegers,
    _
) when erlang:is_integer(Timestamp) ->
  [
    encode_key(Key),
    " ",
    encode_fields(Fields, EncodeIntegers),
    " ",
    erlang:integer_to_list(Timestamp),
    "\n"
  ];

encode({Key, Fields, {Mega, Sec, Micro}}, EncodeIntegers, _) ->
  [
    encode_key(Key),
    " ",
    encode_fields(Fields, EncodeIntegers),
    " ",
    erlang:integer_to_list(
      ?encode_timestamp(Mega, Sec, Micro)
    ),
    "\n"
  ];

encode({Key, Fields, Tags}, EncodeIntegers, SetTimestamp) ->
  [
    encode_key(Key),
    encode_tags(Tags),
    " ",
    encode_fields(Fields, EncodeIntegers),
    if
      SetTimestamp ->
        [" ", erlang:integer_to_list(erlang:system_time(nanosecond))];
      true ->
        ""
    end,
    "\n"
  ];

encode({Key, Fields, Tags, Timestamp}, EncodeIntegers, _) ->
  [
    encode_key(Key),
    encode_tags(Tags),
    " ",
    encode_fields(Fields, EncodeIntegers),
    " ",
    erlang:integer_to_list(Timestamp),
    "\n"
  ];

encode([_|_]=L, EncodeIntegers, SetTimestamp) ->
  encode_list(L, EncodeIntegers, SetTimestamp);

encode(Map, EncodeIntegers, SetTimestamp) when erlang:map_size(Map) > 0 ->
  encode_list(maps:to_list(Map), EncodeIntegers, SetTimestamp);

encode(
    {Key, {Fields, Timestamp}},
    EncodeIntegers,
    _
) when erlang:is_integer(Timestamp) ->
  [
    encode_key(Key),
    " ",
    encode_fields(Fields, EncodeIntegers),
    " ",
    erlang:integer_to_list(Timestamp),
    "\n"
  ];

encode({Key, {Fields, {Mega, Sec, Micro}}}, EncodeIntegers, _) ->
  [
    encode_key(Key),
    " ",
    encode_fields(Fields, EncodeIntegers),
    " ",
    erlang:integer_to_list(
      ?encode_timestamp(Mega, Sec, Micro)
    ),
    "\n"
  ];

encode({Key, {Fields, Tags}}, EncodeIntegers, SetTimestamp) ->
  [
    encode_key(Key),
    encode_tags(Tags),
    " ",
    encode_fields(Fields, EncodeIntegers),
    if
      SetTimestamp ->
        [" ", erlang:integer_to_list(erlang:system_time(nanosecond))];
      true ->
        ""
    end,
    "\n"
  ];

encode({Key, {Fields, Tags, Timestamp}}, EncodeIntegers, _) ->
  [
    encode_key(Key),
    encode_tags(Tags),
    " ",
    encode_fields(Fields, EncodeIntegers),
    " ",
    erlang:integer_to_list(Timestamp),
    "\n"
  ];

encode({Key, Fields}, EncodeIntegers, SetTimestamp) ->
  [
    encode_key(Key),
    " ",
    encode_fields(Fields, EncodeIntegers),
    if
      SetTimestamp ->
        [" ", erlang:integer_to_list(erlang:system_time(nanosecond))];
      true ->
        ""
    end,
    "\n"
  ].


encode_list([Item | Rest], EncodeIntegers, SetTimestamp) ->
  [
    encode(Item, EncodeIntegers, SetTimestamp) |
    encode_list(Rest, EncodeIntegers, SetTimestamp)
  ];

encode_list(_, _, _) ->
  [].


encode_key(Key) when erlang:is_binary(Key) ->
  Key;

encode_key([Char|_]=Key) when erlang:is_integer(Char) ->
  Key;

encode_key(Key) ->
  io_lib:print(Key).


encode_fields({Key, Value}, EncodeIntegers) ->
  [encode_key(Key), "=", encode_value(Value, EncodeIntegers)];

encode_fields([{Key, Value}], EncodeIntegers) ->
  [encode_key(Key), "=", encode_value(Value, EncodeIntegers)];

encode_fields([{_, _}=Item|Rest], EncodeIntegers) ->
  [encode_fields(Item, EncodeIntegers), "," | encode_fields(Rest, EncodeIntegers)];

encode_fields(Tags, EncodeIntegers) when erlang:map_size(Tags) > 0 ->
  encode_fields(maps:to_list(Tags), EncodeIntegers);

encode_fields(Value, EncodeIntegers) ->
  ["value=", encode_value(Value, EncodeIntegers)].


encode_tags({Key, Value}) ->
  ["," , encode_key(Key), "=", encode_key(Value)];

encode_tags([{Key, Value}]) ->
  [",", encode_key(Key), "=", encode_key(Value)];

encode_tags([{_, _}=Item|Rest]) ->
  [encode_tags(Item) | encode_tags(Rest)];

encode_tags(Tags) when erlang:map_size(Tags) > 0 ->
  encode_tags(maps:to_list(Tags));

encode_tags(#{}) ->
  [].


encode_value(Int, EncodeInteger) when erlang:is_integer(Int) ->
  [
    erlang:integer_to_list(Int),
    if
      EncodeInteger ->
        "i";
      true ->
        ""
    end
  ];

encode_value(Float, _) when erlang:is_float(Float) ->
  erlang:float_to_list(Float, [compact, {decimals, 12}]);

encode_value(Value, _) when erlang:is_binary(Value) ->
  Value;

encode_value([Char|_]=Value, _) when erlang:is_integer(Char) -> % assume string
  Value;

encode_value(Atom, _) when erlang:is_atom(Atom) ->
  if
    Atom == true ->
      "t";
    Atom == false ->
      "f";
    true ->
      erlang:atom_to_list(Atom)
  end.
%%;

%%encode_value(_Other, _) ->
%%  "".
