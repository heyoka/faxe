%% Ⓒ 2019
%% @doc
%% Keep only those value(s) with the given fieldnames or tagnames from data_point and data_batch
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
   tags
}).

options() -> [{fields, binary_list, []},{tags, binary_list, []}].

init(NodeId, _Ins, #{fields := Fields, tags := Tags}) ->
   {ok, all, #state{fields = Fields, node_id = NodeId, tags = Tags}}.

process(_In, #data_batch{points = Points} = Batch, State = #state{fields = Fs, tags = Ts}) ->

   NewPoints = lists:map(
      fun(P) -> rewrite(Fs, Ts, P) end,
      Points
   ),
   {emit, Batch#data_batch{points = NewPoints}, State};
process(_Inport, #data_point{} = Point, State = #state{fields = Fs, tags = Ts}) ->
   {emit, rewrite(Point, Fs, Ts), State}.


rewrite(#data_point{} = Point, FieldNames, TagNames) ->
   Fields = flowdata:fields(Point, FieldNames),
   Tags = flowdata:tags(Point, TagNames),
   Point0 = Point#data_point{fields = #{}, tags = # {}},
   NewPoint0 = flowdata:set_fields(Point0, FieldNames, Fields),
   flowdata:set_tags(NewPoint0, TagNames, Tags).


-ifdef(TEST).
rewrite_point_test() ->
   Point = #data_point{ts = 1234, fields = #{<<"value">> => 2134, <<"val44">> => <<"get">>}},
   ?assertEqual(#data_point{ts = 1234, fields = #{<<"val44">> => <<"get">>}},
      rewrite(Point, [<<"val44">>], [])).
-endif.
