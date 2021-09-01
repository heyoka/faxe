%%%-------------------------------------------------------------------
%%% @author heyoka
%%% @copyright (C) 2019, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 09. Sep 2019 13:50
%%%-------------------------------------------------------------------
-module(flowdata_tests).
-author("heyoka").

%% API
-include("faxe.hrl").

-ifdef(TEST).
-compile(nowarn_export_all).
-compile([export_all]).
-include_lib("eunit/include/eunit.hrl").
-import(flowdata,
   [field/2,field/3,delete_field/2,to_json/1,set_field/3,field_names/1,rename_tags/3]).

sample_point() ->
   #data_point{ts = 1231154646, fields=[{f1, 223.3},{f2, 44},{f3, 2}],
      tags=[{t1, <<"hello">>},{t2, <<"JU323z6">>}]}.

path_array_index_test() ->
   faxe_ets:start_link(),
   ?assertEqual(flowdata:path(<<"this[2].is.my.paths[2].id">>),
      {<<"this">>, 2, <<"is">>, <<"my">>, <<"paths">>, 2, <<"id">>}).

path_test() ->
   Path = <<"this.is.my.paths.id">>,
   ?assertEqual(flowdata:path(Path), Path).

get_field_value_kv_test() ->
   P = #data_point{ts = 1234567891234, id = <<"324392i09i329i2df4">>, fields = #{<<"val">> => 3}},
   Path = <<"val">>,
   ?assertEqual(field(P, Path), 3).

get_field_value_undefined_test() ->
   P = #data_point{ts = 1234567891234, id = <<"324392i09i329i2df4">>, fields = #{<<"val">> => 3}},
   Path = <<"value">>,
   ?assertEqual(field(P, Path), undefined).

deep_val() -> maps:from_list([{<<"menu">>,
   {[{<<"id">>,<<"file">>},
      {<<"value">>,<<"File">>},
      {<<"popup">>,
         {[{<<"menuitem">>,
            [{[{<<"value">>,<<"New">>},
               {<<"onclick">>,<<"CreateNewDoc()">>}]},
               {[{<<"value">>,<<"Open">>},{<<"onclick">>,<<"OpenDoc()">>}]},
               {[{<<"value">>,<<"Close">>},
                  {<<"onclick">>,<<"CloseDoc()">>},
                  {<<"ondbclick">>, <<"print()">>}
               ]}]}]}}]}}]).


get_field_value_deep_test() ->
   P = #data_point{ts = 1234567891234, id = <<"324392i09i329i2df4">>, fields = #{<<"val">> => deep_val()}},
   Path = <<"val.menu.popup.menuitem">>,
   ?assertEqual(field(P, Path),
      [{[{<<"value">>,<<"New">>},
         {<<"onclick">>,<<"CreateNewDoc()">>}]},
         {[{<<"value">>,<<"Open">>},{<<"onclick">>,<<"OpenDoc()">>}]},
         {[{<<"value">>,<<"Close">>},
            {<<"onclick">>,<<"CloseDoc()">>},
            {<<"ondbclick">>, <<"print()">>}
         ]}]
   ).

delete_field_value_deep_test() ->
   P = #data_point{ts = 1234567891234, id = <<"324392i09i329i2df4">>,
      fields = #{<<"val">> => deep_val(), <<"var">> => 44}},
   Path = <<"val.menu.popup.menuitem">>,
   NewPoint = delete_field(P, Path),
   ?assertEqual(NewPoint#data_point.fields,
      #{<<"val">> =>
      #{<<"menu">> =>
      {[{<<"id">>,<<"file">>},
         {<<"value">>,<<"File">>},
         {<<"popup">>,{[]}}]}},
         <<"var">> => 44}
   ).

delete_field_undefined_test() ->
   P = #data_point{ts = 1234567891234, id = <<"324392i09i329i2df4">>,
      fields = #{<<"val">> => <<"some_string_value">>, <<"var">> => 44}},
   Path = {<<"vuzz">>, <<"vizz">>, <<"vazz">>},
   NewPoint = flowdata:delete_field(P, Path),
   ?assertEqual(NewPoint#data_point.fields, P#data_point.fields).

delete_fields_test() ->
   P = #data_point{ts = 1234567891234, id = <<"324392i09i329i2df4">>,
      fields = #{<<"val">> => deep_val(), <<"var">> => 44}},
   Paths = [<<"val.menu.popup.menuitem">>, <<"var">>],
   NewPoint = flowdata:delete_fields(P, Paths),
   ?assertEqual(NewPoint#data_point.fields,
      #{<<"val">> =>
      #{<<"menu">> =>
      {[{<<"id">>,<<"file">>},
         {<<"value">>,<<"File">>},
         {<<"popup">>,{[]}}]}} }
   ).

delete_fields_empty_test() ->
   P = #data_point{ts = 1234567891234, id = <<"324392i09i329i2df4">>,
      fields = #{<<"val">> => deep_val(), <<"var">> => 44}},
   Paths = [],
   NewPoint = flowdata:delete_fields(P, Paths),
   ?assertEqual(NewPoint#data_point.fields, P#data_point.fields).

delete_fields_undefined_test() ->
   P = #data_point{ts = 1234567891234, id = <<"324392i09i329i2df4">>,
      fields = #{<<"val">> => deep_val(), <<"var">> => 44}},
   Paths = [<<"pops.pips.paps">>, <<"var">>],
   NewPoint = flowdata:delete_fields(P, Paths),
   ?assertEqual(NewPoint,
       #data_point{ts = 1234567891234, id = <<"324392i09i329i2df4">>,
         fields = #{<<"val">> => deep_val()}
      }

   ).

rename_field_basic_test() ->
   P = #data_point{ts = 1234567891234, id = <<"324392i09i329i2df4">>,
      fields = #{<<"val">> => <<"somestring">>, <<"var">> => 44}},
   From = [<<"val">>], To = [<<"value.into.deep">>],
   SetP = flowdata:rename_fields(P, From, To),
   ?assertEqual(SetP#data_point.fields,
      #{<<"value">> => #{<<"into">> => #{<<"deep">> => <<"somestring">>}}, <<"var">> => 44}).

rename_field_undefined_test() ->
   P = #data_point{ts = 1234567891234, id = <<"324392i09i329i2df4">>,
      fields = #{<<"val">> => <<"somestring">>, <<"var">> => 44}},
   From = [<<"val">>, <<"value">>], To = [<<"value.into.deep">>, <<"my_value">>],
   SetP = flowdata:rename_fields(P, From, To),
   ?assertEqual(SetP#data_point.fields,
      #{<<"value">> => #{<<"into">> => #{<<"deep">> => <<"somestring">>}}, <<"var">> => 44}).

rename_field_empty_path_test() ->
   P = #data_point{ts = 1234567891234, id = <<"324392i09i329i2df4">>,
      fields = #{<<"val">> => <<"somestring">>, <<"var">> => 44}},
   From = [], To = [],
   SetP = flowdata:rename_fields(P, From, To),
   ?assertEqual(SetP#data_point.fields, P#data_point.fields).

set_field_kv_test() ->
   P = #data_point{ts = 1234567891234, id = <<"324392i09i329i2df4">>,
      fields = #{<<"val">> => <<"somestring">>, <<"var">> => 44}},
   Path = <<"value">>,
   SetP = set_field(P, Path, <<"new">>),
   ?assertEqual(#{<<"val">> => <<"somestring">>,
         <<"var">> => 44, <<"value">> => <<"new">>},
      SetP#data_point.fields).

set_field_deep_test() ->
   P = #data_point{ts = 1234567891234, id = <<"324392i09i329i2df4">>,
      fields = #{<<"val">> => <<"somestring">>, <<"var">> => 44}},
   Path = <<"value.into.deep">>,
   SetP = set_field(P, Path, <<"new">>),
   ?assertEqual(SetP#data_point.fields,
      #{<<"val">> => <<"somestring">>, <<"var">> => 44,
         <<"value">> =>
            #{<<"into">> =>
               #{<<"deep">> => <<"new">>}}}
   ).

set_field_array_test() ->
   faxe_ets:start_link(),
   P = #data_point{ts = 1234567891234},
   Path = <<"data.mr[1].value">>,
   SetP = set_field(P, Path, 33),
   ?assertEqual(
      #{<<"data">> => #{<<"mr">> => [#{<<"value">> => 33}]}},
      SetP#data_point.fields
   ).

set_multiple_fields_test() ->
   P = #data_point{ts = 1234567891234, id = <<"324392i09i329i2df4">>,
      fields = #{<<"val">> => <<"somestring">>, <<"var">> => 44}},
   Keys = [<<"simple">>, <<"value.into.deep">>],
   Values = [321321.5645, <<"some_string-value">>],
   SetP = flowdata:set_fields(P, Keys, Values),
   ?assertEqual(SetP#data_point.fields,
      #{<<"val">> => <<"somestring">>, <<"var">> => 44,
         <<"simple">> => 321321.5645,
         <<"value">> =>
         #{<<"into">> =>
         #{<<"deep">> => <<"some_string-value">>}}}
   ).

set_multiple_fields_kvlist_test() ->
   P = #data_point{ts = 1234567891234, id = <<"324392i09i329i2df4">>,
      fields = #{<<"val">> => <<"somestring">>, <<"var">> => 44}},
   Keys = [<<"simple">>, <<"value.into.deep">>],
   Values = [321321.5645, <<"some_string-value">>],
   KVs = lists:zip(Keys, Values),
   SetP = flowdata:set_fields(P, KVs),
   ?assertEqual(SetP#data_point.fields,
      #{<<"val">> => <<"somestring">>, <<"var">> => 44,
         <<"simple">> => 321321.5645,
         <<"value">> =>
         #{<<"into">> =>
         #{<<"deep">> => <<"some_string-value">>}}}
   ).

set_multiple_fields_kvlist_tuple_path_test() ->
   P = #data_point{ts = 1234567891234, id = <<"324392i09i329i2df4">>,
      fields = #{<<"val">> => <<"somestring">>, <<"var">> => 44}},
   Keys = [flowdata:path(E) || E <- [<<"simple">>, <<"value.into.deep">>]],
   Values = [321321.5645, <<"some_string-value">>],
   KVs = lists:zip(Keys, Values),
   SetP = flowdata:set_fields(P, KVs),
   ?assertEqual(SetP#data_point.fields,
      #{<<"val">> => <<"somestring">>, <<"var">> => 44,
         <<"simple">> => 321321.5645,
         <<"value">> =>
         #{<<"into">> =>
         #{<<"deep">> => <<"some_string-value">>}}}
   ).

set_multiple_tags_test() ->
   P = #data_point{ts = 1234567891234, id = <<"324392i09i329i2df4">>,
      tags = #{<<"tag1">> => <<"somestring">>, <<"tag2">> => <<"44">>}},
   Keys = [<<"tag3">>, <<"tag4">>],
   Values = [<<"mytagvalue">>, <<"some_string-value">>],
   SetP = flowdata:set_tags(P, Keys, Values),
   ?assertEqual(SetP#data_point.tags,
      #{<<"tag1">> => <<"somestring">>, <<"tag2">> => <<"44">>,
         <<"tag3">> => <<"mytagvalue">>,
         <<"tag4">> => <<"some_string-value">>}
   ).

rename_tag_kv_test() ->
   P = #data_point{ts = 1234567891234, id = <<"324392i09i329i2df4">>,
      tags = #{<<"val">> => <<"somestring">>, <<"var">> => <<"anotherstring">>}},
   From = [<<"var">>, <<"val">>], To = [<<"variable">>, <<"value">>],
   SetP = rename_tags(P, From, To),
   ?assertEqual(SetP#data_point.tags,
      #{<<"value">> => <<"somestring">>, <<"variable">> => <<"anotherstring">>}).

%% array indices are available only through the tuple path format
%%%
r_val() -> #{<<"foo">> => #{<<"bar">> =>[#{<<"first">> => 1},2,3]}}.

rename_field_deep_array_index_test() ->
   P = #data_point{ts = 1234567891234, id = <<"324392i09i329i2df4">>,
      fields = #{<<"val">> => r_val(), <<"var">> => 44}},
   From = [<<"val.foo.bar[1].first">>],
   To = [<<"val.foo.bar[1].erster">>],
   SetP = flowdata:rename_fields(P, From, To),
   ?assertEqual(SetP#data_point.fields,
      #{<<"val">> => #{<<"foo">> => #{<<"bar">> =>[#{<<"erster">> => 1},2,3]}}, <<"var">> => 44}
   ).

get_field_names_kv_test() ->
   P = #data_point{ts = 1234567891234, id = <<"324392i09i329i2df4">>,
      fields = #{<<"val">> => <<"somestring">>, <<"var">> => 44}},
   RList = field_names(P),
   ?assert(lists:member(<<"var">>, RList)),
   ?assert(lists:member(<<"val">>, RList)).
%%   ?assertEqual(field_names(P), [<<"var">>, <<"val">>]).

get_field_names_deep_test() ->
   P = #data_point{ts = 1234567891234, id = <<"324392i09i329i2df4">>,
      fields = #{<<"val">> => deep_val(),<<"var">> =>44}},
   RList = field_names(P),
   ?assert(lists:member(<<"var">>, RList)),
   ?assert(lists:member(<<"val">>, RList)).


json_basic_test() ->
   P = #data_point{ts = 1568029511598, fields = #{<<"id">> => <<"ioi2u34oiu23oi4u2oi4u2">>,
      <<"df">> => <<"01.002">>, <<"value1">> => 323424, <<"value2">> => <<"somestringvalue">>}},
   ?assertEqual(jiffy:decode(to_json(P), [return_maps]),
      #{<<"ts">> =>1568029511598,
         <<"id">> => <<"ioi2u34oiu23oi4u2oi4u2">>,
         <<"df">> => <<"01.002">>,
         <<"value1">> => 323424, <<"value2">> => <<"somestringvalue">>}

   ).

json_basic_vs_test() ->
   P = #data_point{ts = 1568029511598, fields = #{<<"id">> => <<"ioi2u34oiu23oi4u2oi4u2">>,
      <<"df">> => <<"01.002">>,  <<"value1">> => 323424,
      <<"value2">> => <<"somestringvalue">>}},
   ?assertEqual(jiffy:decode(to_json(P), [return_maps]),
      #{<<"ts">> =>1568029511598,
         <<"id">> => <<"ioi2u34oiu23oi4u2oi4u2">>,
         <<"df">> => <<"01.002">>,
         <<"value1">> => 323424, <<"value2">> => <<"somestringvalue">>}

   ).

json_basic_datamap_test() ->
   P = #data_point{ts = 1568029511598, fields = #{<<"id">> => <<"ioi2u34oiu23oi4u2oi4u2">>,
      <<"df">> => <<"01.002">>, <<"value1">> => 323424,
      <<"data">> => #{<<"value2">> => <<"somestringvalue">>}}
   },
   ?assertEqual(jiffy:decode(to_json(P), [return_maps]),
      #{<<"ts">> =>1568029511598,
         <<"id">> => <<"ioi2u34oiu23oi4u2oi4u2">>,
         <<"df">> => <<"01.002">>, <<"value1">> => 323424,
         <<"data">> => #{<<"value2">> => <<"somestringvalue">>}}

   ).

json_basic_default_test() ->
   P = #data_point{ts = 1568029511598,
      fields = #{<<"value1">> => 323424, <<"value2">> => <<"somestringvalue">>}},
   ?assertEqual(jiffy:decode(to_json(P), [return_maps]),
      #{<<"ts">> =>1568029511598,<<"value1">> => 323424, <<"value2">> => <<"somestringvalue">>}
   ).

json_basic_data_test() ->
   P = #data_point{ts = 1568029511598, fields =
   #{<<"id">> => <<"ioi2u34oiu23oi4u2oi4u2">>,
      <<"df">> => <<"01.002">>,
      <<"data">> => #{<<"value1">> => 323424, <<"value2">> => <<"somestringvalue">>}}
   },

   ?assertEqual(jiffy:decode(to_json(P), [return_maps]),

      #{<<"ts">> =>1568029511598,
         <<"id">> => <<"ioi2u34oiu23oi4u2oi4u2">>,
         <<"df">> => <<"01.002">>,
         <<"data">> => #{<<"value1">> => 323424, <<"value2">> => <<"somestringvalue">>}}


   ).

json_basic_data_excl_test() ->
   P = #data_point{ts = 1568029511598, fields =
      #{<<"id">> => <<"ioi2u34oiu23oi4u2oi4u2">>,
         <<"df">> => <<"01.002">>, <<"value1">> => 2323422, <<"value2">> => <<"savoi">>,
         <<"data">> => #{<<"value1">> => 323424, <<"value2">> => <<"somestringvalue">>}}
   },
   ?assertEqual(jiffy:decode(to_json(P), [return_maps]),
      #{<<"ts">> =>1568029511598,
         <<"id">> => <<"ioi2u34oiu23oi4u2oi4u2">>,
         <<"df">> => <<"01.002">>, <<"value1">> => 2323422, <<"value2">> => <<"savoi">>,
         <<"data">> => #{<<"value1">> => 323424, <<"value2">> => <<"somestringvalue">>}}

   ).


batch_to_json_test() ->
   P = #data_point{ts = 1568029511598, fields =
   #{<<"id">> => <<"ioi2u34oiu23oi4u2oi4u2">>,
      <<"df">> => <<"01.002">>,  <<"value1">> => 2323422, <<"value2">> => <<"savoi">>,
      <<"data">> => #{<<"value1">> => 323424, <<"value2">> => <<"somestringvalue">>}}
   },
   Points = [P#data_point{ts = P#data_point.ts+(X*1000)} || X <- lists:seq(1,5)],
   Json = to_json(#data_batch{points = Points}),
   ?assertEqual(jiffy:decode(Json, [return_maps]),
      [#{<<"data">> =>
      #{<<"value1">> => 323424,
         <<"value2">> => <<"somestringvalue">>},
         <<"df">> => <<"01.002">>,
         <<"id">> => <<"ioi2u34oiu23oi4u2oi4u2">>,
         <<"ts">> => 1568029512598,<<"value1">> => 2323422,
         <<"value2">> => <<"savoi">>},
         #{<<"data">> =>
         #{<<"value1">> => 323424,
            <<"value2">> => <<"somestringvalue">>},
            <<"df">> => <<"01.002">>,
            <<"id">> => <<"ioi2u34oiu23oi4u2oi4u2">>,
            <<"ts">> => 1568029513598,<<"value1">> => 2323422,
            <<"value2">> => <<"savoi">>},
         #{<<"data">> =>
         #{<<"value1">> => 323424,
            <<"value2">> => <<"somestringvalue">>},
            <<"df">> => <<"01.002">>,
            <<"id">> => <<"ioi2u34oiu23oi4u2oi4u2">>,
            <<"ts">> => 1568029514598,<<"value1">> => 2323422,
            <<"value2">> => <<"savoi">>},
         #{<<"data">> =>
         #{<<"value1">> => 323424,
            <<"value2">> => <<"somestringvalue">>},
            <<"df">> => <<"01.002">>,
            <<"id">> => <<"ioi2u34oiu23oi4u2oi4u2">>,
            <<"ts">> => 1568029515598,<<"value1">> => 2323422,
            <<"value2">> => <<"savoi">>},
         #{<<"data">> =>
         #{<<"value1">> => 323424,
            <<"value2">> => <<"somestringvalue">>},
            <<"df">> => <<"01.002">>,
            <<"id">> => <<"ioi2u34oiu23oi4u2oi4u2">>,
            <<"ts">> => 1568029516598,<<"value1">> => 2323422,
            <<"value2">> => <<"savoi">>}]


   ).

empty_data_test() ->
   P = #data_point{ts = 1568029511598},
   ?assertEqual(
      maps:new(),
      flowdata:to_mapstruct(P)
   ).

empty_data_json_test() ->
   P = #data_point{ts = 1568029511598, fields =
   #{<<"id">> => <<"ioi2u34oiu23oi4u2oi4u2">>,
      <<"df">> => <<"01.002">>,
      <<"data">> => #{}}
   },
   Json = to_json(P),
   ?assertEqual(
      P,
      flowdata:from_json_struct(Json)
      ).


from_json_basic_test() ->
   M = #{<<"ts">> => 1568029511598, <<"id">> => <<"ioi2u34oiu23oi4u2oi4u2">>,
      <<"df">> => <<"01.002">>, <<"vs">> => 2, <<"value1">> => 2323422, <<"value2">> => <<"savoi">>,
      <<"data">> => #{<<"value1">> => 323424, <<"value2">> => <<"somestringvalue">>}},
   JSON = jiffy:encode(M),
   ?assertEqual(flowdata:from_json_struct(JSON),
      #data_point{ts=1568029511598, fields = #{<<"id">> => <<"ioi2u34oiu23oi4u2oi4u2">>,
   <<"df">> => <<"01.002">>, <<"vs">> => 2, <<"value1">> => 2323422, <<"value2">> => <<"savoi">>,
   <<"data">> => #{<<"value1">> => 323424, <<"value2">> => <<"somestringvalue">>}}
      }
      ).

from_json_basic2_test() ->
   M = #{<<"UTC-Time">> => <<"1568029511.598123">>, <<"id-string">> => <<"ioi2u34oiu23oi4u2oi4u2">>,
      <<"raw">> => <<"01002">>, <<"vsdtg">> => 22,
      <<"value1">> => 323424, <<"value2">> => <<"somestringvalue">>},
   JSON = jiffy:encode(M),
   lager:notice("~s",[JSON]),
   ?assertEqual(
      #data_point{ts=1568029511598, fields = #{<<"UTC-Time">> => <<"1568029511.598123">>,
         <<"id-string">> => <<"ioi2u34oiu23oi4u2oi4u2">>,
         <<"raw">> => <<"01002">>,
         <<"value1">> => 323424,
         <<"value2">> => <<"somestringvalue">>,
         <<"vsdtg">> => 22}
      }, flowdata:from_json_struct(JSON, <<"UTC-Time">>, ?TF_TS_FLOAT_MICRO)
   ).

from_json_basic3_test() ->
   M = #{<<"datetime">> => <<"2019-10-05T14:48:01.023Z">>, <<"id-string">> => <<"ioi2u34oiu23oi4u2oi4u2">>,
      <<"raw">> => <<"01002">>, <<"vsdtg">> => 22,
      <<"value1">> => 323424, <<"value2">> => #{<<"key12">> => #{<<"anotherkey">> => <<"somestringvalue">>}}
   },
   JSON = jiffy:encode(M),
   lager:notice("~s",[JSON]),
   ?assertEqual(
      #data_point{ts=1570286881023,
         fields = #{<<"datetime">> =>
         <<"2019-10-05T14:48:01.023Z">>,
            <<"id-string">> => <<"ioi2u34oiu23oi4u2oi4u2">>,
            <<"raw">> => <<"01002">>,
            <<"value1">> => 323424,
            <<"value2">> =>
            #{<<"key12">> =>
               #{<<"anotherkey">> =>
                  <<"somestringvalue">>}},
            <<"vsdtg">> => 22}
      }, flowdata:from_json_struct(JSON, <<"datetime">>, ?TF_ISO8601)
   ).

from_json_list_test() ->
   M = [#{<<"datetime">> => <<"2019-10-05T14:48:01.023Z">>, <<"id-string">> => <<"ioi2u34oiu23oi4u2oi4u2">>,
      <<"raw">> => <<"01002">>, <<"vsdtg">> => 22,
      <<"value1">> => 323424, <<"value2">> => #{<<"key12">> => #{<<"anotherkey">> => <<"somestringvalue">>}}
   }],
   JSON = jiffy:encode(M),
   lager:notice("~s",[JSON]),
   ?assertEqual(
      #data_batch{points = [#data_point{
         ts=1570286881023,
         fields = #{<<"datetime">> =>
         <<"2019-10-05T14:48:01.023Z">>,
            <<"id-string">> => <<"ioi2u34oiu23oi4u2oi4u2">>,
            <<"raw">> => <<"01002">>,
            <<"value1">> => 323424,
            <<"value2">> =>
            #{<<"key12">> =>
            #{<<"anotherkey">> =>
            <<"somestringvalue">>}},
            <<"vsdtg">> => 22}}]
      }, flowdata:from_json_struct(JSON, <<"datetime">>, ?TF_ISO8601)
   ).


from_map_basic_test() ->
   Map = #{<<"df">> => <<"02.005">>,<<"double_val">> => 10.220761769454324,
      <<"id">> => <<"oi23u4oi23u4oi32u34oi2u3">>,<<"ts">> => 1573546741698,
      <<"val">> => 5.110380884727162,<<"vs">> => 1},
   ?assertEqual(flowdata:point_from_json_map(Map),
      #data_point{ts=1573546741698, fields=
   #{<<"df">> => <<"02.005">>,<<"double_val">> => 10.220761769454324,
      <<"id">> => <<"oi23u4oi23u4oi32u34oi2u3">>,
      <<"val">> => 5.110380884727162,<<"vs">> => 1}}
      ).


msgpack_basic_test() ->
   P = #data_point{ts = 1568029511598, fields = #{<<"id">> => <<"ioi2u34oiu23oi4u2oi4u2">>,
      <<"df">> => <<"01.002">>,
      <<"data">> => #{<<"value1">> => 323424, <<"value2">> => <<"somestringvalue">>}}
   },
   ?assertEqual(msgpack:unpack(flowdata:to_s_msgpack(P),[{map_format,map}]),
      {ok,#{<<"ts">> =>1568029511598,
         <<"id">> => <<"ioi2u34oiu23oi4u2oi4u2">>,
         <<"df">> => <<"01.002">>,
         <<"data">> => #{<<"value1">> => 323424, <<"value2">> => <<"somestringvalue">>}}
      }
   ).


root_path_bin_test() ->
   ?assertEqual(true, flowdata:is_root_path(<<"root">>)),
   ?assertEqual(false, flowdata:is_root_path(<<"root.path">>)),
   ?assertEqual(false, flowdata:is_root_path(<<"root[2]">>)),
   ?assertEqual(true, flowdata:is_root_path({<<"root_path">>})),
   ?assertEqual(false, flowdata:is_root_path(<<"root.a.path[13]">>)),
   ?assertEqual(false, flowdata:is_root_path({<<"root">>, <<"path">>})).


clean_keys_test() ->
   Map =
      #{<<"df.">> => <<"02.005">>,<<"double_val">> => 10.220761769454324,
      <<"id">> => <<"oi23u4oi23u4oi32u34oi2u3">>,<<"ts">> => 1573546741698,
      <<"val">> => 5.110380884727162,<<"this.is.a.key.with.points">> => <<"val.with.points">>},
   Expected =
      #{<<"df_">> => <<"02.005">>,<<"double_val">> => 10.220761769454324,
         <<"id">> => <<"oi23u4oi23u4oi32u34oi2u3">>,<<"ts">> => 1573546741698,
         <<"val">> => 5.110380884727162,<<"this_is_a_key_with_points">> => <<"val.with.points">>},
   ?assertEqual(Expected, flowdata:clean_field_keys(Map)).

set_root_undefined_test() ->
   P = #data_point{ts = 1, fields = #{<<"field1">> => 33}},
   ?assertEqual(P, flowdata:set_root(P, undefined)).

set_root_not_present_test() ->
   P = #data_point{ts = 1, fields = #{<<"field1">> => 33}},
   Expect = P#data_point{fields = #{<<"new_root">> => #{<<"field1">> => 33}}},
   ?assertEqual(Expect, flowdata:set_root(P, <<"new_root">>)).

set_root_not_present_deep_test() ->
   P = #data_point{ts = 1, fields = #{<<"field1">> => 33}},
   Expect = P#data_point{fields = #{<<"new_root">> => #{<<"new_sub_root">> => #{<<"field1">> => 33}}}},
   ?assertEqual(Expect, flowdata:set_root(P, <<"new_root.new_sub_root">>)).

set_root_present_test() ->
   P = #data_point{fields = #{<<"root">> => #{<<"field1">> => 33}}},
   ?assertEqual(P, flowdata:set_root(P, <<"root">>)).

set_root_present_deep_test() ->
   P = #data_point{fields = #{<<"root">> => #{<<"subroot">> => #{<<"field1">> => 33}}}},
   ?assertEqual(P, flowdata:set_root(P, <<"root.subroot">>)).

-endif.
