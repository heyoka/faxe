%% Ⓒ 2019
%% @doc
%% Keep only those value(s) with the given field or tag paths from data_point and data_batch
%% @end
-module(esp_keep).
-author("Alexander Minichmair").

%% API
-behavior(df_component).

-include("faxe.hrl").
%% API
-export([init/3, process/3, options/0]).

-record(state, {
   node_id,
   fields,
   tags,
   as_fields
}).

options() -> [{fields, binary_list, []}, {tags, binary_list, []}, {as, string_list, []}].

init(NodeId, _Ins, #{fields := Fields, tags := Tags, as := As}) ->
   {ok, all, #state{fields = Fields, node_id = NodeId, tags = Tags, as_fields = As}}.

process(_In, #data_batch{points = Points} = Batch, State = #state{fields = Fs, tags = Ts, as_fields = As}) ->

   NewPoints = lists:map(
      fun(P) -> rewrite(P, Fs, Ts, As) end,
      Points
   ),
   {emit, Batch#data_batch{points = NewPoints}, State};
process(_Inport, #data_point{} = Point, State = #state{fields = Fs, tags = Ts, as_fields = As}) ->
   lager:info("+++ KEEP got: ~p",[lager:pr(Point, ?MODULE)]),
   {emit, rewrite(Point, Fs, Ts, As), State}.

rewrite(#data_point{} = Point, FieldNames, TagNames, []) ->
   rewrite(Point, FieldNames, TagNames, FieldNames);
rewrite(#data_point{} = Point, FieldNames, TagNames, Aliases) ->
   Fields = flowdata:fields(Point, FieldNames),
   Tags = flowdata:tags(Point, TagNames),
   Point0 = Point#data_point{fields = #{}, tags = # {}},
   NewPoint0 = flowdata:set_fields(Point0, Aliases, Fields),
   flowdata:set_tags(NewPoint0, TagNames, Tags).


-ifdef(TEST).
rewrite_point_test() ->
   Point = #data_point{ts = 1234, fields = #{<<"value">> => 2134, <<"val44">> => <<"get">>}},
   ?assertEqual(#data_point{ts = 1234, fields = #{<<"val">> => <<"get">>}},
      rewrite(Point, [<<"val44">>], [], [<<"val">>])).
rewrite_points_path_test() ->
   Point = #data_point{ts = 1234, fields = #{<<"first">> => #{<<"value">> => 2134, <<"val44">> => <<"get">>}}},
   ?assertEqual(#data_point{ts = 1234, fields = #{<<"val">> => <<"get">>}},
      rewrite(Point, [<<"first.val44">>], [], [<<"val">>])).
rewrite_points_path_no_aliases_test() ->
   Point = #data_point{ts = 1234, fields = #{<<"first">> => #{<<"value">> => 2134, <<"val44">> => <<"get">>}}},
   ?assertEqual(#data_point{ts = 1234, fields = #{<<"first">> => #{<<"val44">> => <<"get">>}}},
      rewrite(Point, [<<"first.val44">>], [], [])).
rewrite_points_path_alias_path_test() ->
   Point = #data_point{ts = 1234, fields = #{<<"first">> => #{<<"value">> => 2134, <<"val44">> => <<"get">>}}},
   ?assertEqual(#data_point{ts = 1234, fields = #{<<"erster">> => #{<<"val">> => <<"get">>}}},
      rewrite(Point, [<<"first.val44">>], [], [<<"erster.val">>])).
-endif.
