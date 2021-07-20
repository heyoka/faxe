%% Date: 05.01.17 - 10:21
%% Ⓒ 2017 heyoka
%% @todo implement align
-module(esp_json_emitter).
-author("Alexander Minichmair").

-behaviour(df_component).

-include("faxe.hrl").
%% API
-export([init/3, process/3, handle_info/2, options/0, params/0, check_options/0, check_json/1]).
-record(state, {
   node_id           :: term(),
   every             :: non_neg_integer(),
   jitter            :: non_neg_integer(),
   align             :: atom(),
   json_string       :: binary(),
   ejson             :: map()|list(),
   as                :: binary()
}).

params() -> [].

options() ->
   [{every, duration, <<"3s">>},
      {jitter, duration, <<"0ms">>},
      {align, is_set},
      {json, binary_list, undefined},
      {rand_fields, binary_list, []},
      {as, string, <<"data">>}].


check_options() ->
   [{func, json, fun check_json/1, <<", invalid json">>}].

check_json(Jsons) when is_list(Jsons) ->
   lists:all(fun check_json/1, Jsons);
check_json(Json) when is_binary(Json) ->
   case catch jiffy:decode(Json) of
      {'EXIT', _} -> false;
      _Other -> true
   end.

init(NodeId, _Inputs,
    #{every := Every, align := Unit, jitter := Jitter, json := JS, as := As}) ->
   NUnit =
      case Unit of
         false -> false;
         true -> faxe_time:binary_to_duration(Every)
      end,
   JT = faxe_time:duration_to_ms(Jitter),
   EveryMs = faxe_time:duration_to_ms(Every),
   JSONs = [jiffy:decode(JsonString, [return_maps]) || JsonString <- JS],
   State = #state{node_id = NodeId, every = EveryMs, ejson = JSONs, as = As,
      json_string = JS, align = NUnit, jitter = JT},

   erlang:send_after(JT, self(), emit),
   rand:seed(exs1024s),
   {ok, none, State}.


process(_Inport, _Value, State) ->
   {ok, State}.

handle_info(emit, State=#state{every = Every, jitter = JT, ejson = JS, as = As}) ->
   Jitter = round(rand:uniform()*JT),
   After = Every+(Jitter),
   erlang:send_after(After, self(), emit),
   JsonMap = lists:nth(rand:uniform(length(JS)), JS),
   Msg = flowdata:set_field(#data_point{ts = faxe_time:now()}, As, JsonMap),

   lager:notice("~p", [obj_from_array(Msg)]),

   {emit,{1, Msg}, State};
handle_info(_Request, State) ->
   {ok, State}.

obj_from_array(Point) ->
   Res = obj_from_array(Point, <<"data.sections">>, <<>>, <<"name">>),
   obj_from_array(Res, <<"data.sections">>, <<"inventoryLine">>, <<"sku">>).

obj_from_array(Point = #data_point{}, Path, SubPath, Key) ->
   Array0 = flowdata:field(Point, Path),
   lager:notice("THE FIELD: ~p",[Array0]),
   {Arrays, BasePaths} =
   case is_list(Array0) of
      true -> {[Array0], [Path]};
      false -> %% its a map
         PathList = [<<AKey/binary, ".", SubPath/binary>> || AKey <- maps:keys(Array0)],
         lager:info("pathlist: ~p",[PathList]),
         As = jsn:get_list(PathList, Array0),
         lager:notice("ArraYs: ~p", [As]),
         BPaths = [<<Path/binary, ".", SFix/binary>> || SFix <- PathList],
         {As, BPaths}
   end,

   lager:info("array: ~p",[Arrays]),
   lager:info("basepaths: ~p",[BasePaths]),
   lists:foldl(
      fun
         %% if we have not found an array, skip the transfrom
         ({_, undefined}, Point) -> Point;

         ({BasePath, ArrayEntry}, Point) ->
         transform(Point, ArrayEntry, BasePath, Key)
      end,
      Point,
      lists:zip(BasePaths, Arrays)
   ).

transform(Point, Array, Path, Key) ->
   %% get the new subobject keys and the array entries (objects)
   lager:info("path is: ~p",[Path]),
   lager:warning("array is : ~p",[Array]),
   Selected = jsn:select([{value, Key}, identity], Array),
   lager:notice("Selections: ~p",[Selected]),
   OutPrep =
      lists:map(fun([KeyVal, Contents0]) ->

         SelPath = <<Path/binary, ".", KeyVal/binary>>,
         {SelPath, jsn:delete(Key, Contents0)}
                end,
         Selected),
   NewPoint = flowdata:delete_field(Point, Path),
   flowdata:set_fields(NewPoint, OutPrep).




