%%%-------------------------------------------------------------------
%%% @author heyoka
%%% @copyright (C) 2020, <COMPANY>
%%% @doc
%%%
%%% @end
%%% @todo rename functions
%%% Created : 27. Jan 2020 20:17
%%%-------------------------------------------------------------------
-module(string_template).
-author("heyoka").

-include("faxe.hrl").

%% API
-export([extract/1, to_fun/1, to_string/2, eval/2]).

%% {{My name is "name"}}

%% evaluate a template with the given datapoint
-spec eval(binary(), #data_point{}) -> binary().
eval(Template, DataPoint) ->
   Fun = to_fun(extract(Template)),
   to_string(Fun, DataPoint).

%% parse
extract(Template) when is_binary(Template) ->
   Matches = re:run(Template, "{{([a-zA-Z0-9\.\\[\\]_-]*)}}", [global, {capture, all, binary}]),
   case Matches of
      nomatch -> Template;
      {match, Matched} ->
         Res0 = [{TVar, Var} || [TVar, Var] <- Matched],
         {Replace, Vars} = lists:unzip(Res0),
         Format = binary_to_list(binary:replace(Template, Replace, <<"~s">>, [global])),
%%         io:format(Format, Vars),
         {Format, Vars}
   end.

to_fun(Val) when is_binary(Val) -> Val;
to_fun({Format, Vars}) ->
   fun(Point = #data_point{}) ->
      Fields0 = flowdata:fields(Point, Vars),
      Fields = [conv(F) || F <- Fields0],
      list_to_binary(io_lib:format(Format, Fields))
   end.

%% evaluate
to_string(Fun, Point) when is_function(Fun) ->
   Fun(Point);
to_string(Val, _Point) ->
   Val.

conv(V) when is_float(V) ->
   float_to_binary(V, [{decimals, 6}]);
conv(V) when is_integer(V) ->
   integer_to_binary(V);
conv(V) -> V.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-ifdef(TEST).
basic_test() ->
   Template = <<"We have {{tries}} tries so far and {{left}} tries left.">>,
   Point = #data_point{fields = #{<<"tries">> => 2, <<"left">> => 3}},
   Fun = to_fun(extract(Template)),
   ?assertEqual(
      <<"We have 2 tries so far and 3 tries left.">>,
      to_string(Fun, Point)
   ).

no_template_test() ->
   Template = <<"We have 5 tries so far and none tries left.">>,
   Point = #data_point{fields = #{<<"tries">> => 2, <<"left">> => 3}},
   Fun = to_fun(extract(Template)),
   ?assertEqual(
      <<"We have 5 tries so far and none tries left.">>,
      to_string(Fun, Point)
   ).

-endif.
