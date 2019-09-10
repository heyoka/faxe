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

set_field_kv_test() ->
   P = #data_point{ts = 1234567891234, id = <<"324392i09i329i2df4">>,
      fields = #{<<"val">> => <<"somestring">>, <<"var">> => 44}},
   Path = <<"value">>,
   SetP = set_field(P, Path, <<"new">>),
   ?assertEqual(SetP#data_point.fields,
      #{<<"val">> => <<"somestring">>, <<"var">> => 44, <<"value">> => <<"new">>}).

rename_tag_kv_test() ->
   P = #data_point{ts = 1234567891234, id = <<"324392i09i329i2df4">>,
      tags = #{<<"val">> => <<"somestring">>, <<"var">> => <<"anotherstring">>}},
   From = [<<"var">>, <<"val">>], To = [<<"variable">>, <<"value">>],
   SetP = rename_tags(P, From, To),
   ?assertEqual(SetP#data_point.tags,
      #{<<"value">> => <<"somestring">>, <<"variable">> => <<"anotherstring">>}).

%% array indices are available only through the tuple path format
%%%
%%rename_field_deep_test() ->
%%   P = #data_point{ts = 1234567891234, id = <<"324392i09i329i2df4">>,
%%      fields = [{<<"val">>, deep_val()},{<<"var">>,44}]},
%%   From = [<<"val.menu.popup.menuitem">>, <<"val.menu.popup.menuitem[1].value">>],
%%   To = [<<"val.menu.popup.menu_item">>, <<"val.menu.popup.menuitem[1].val">>],
%%   SetP = rename_fields(P, From, To),
%%   ?assertEqual(SetP#data_point.fields,
%%      [{<<"val">>,
%%         {[{<<"menu">>,
%%            {[{<<"id">>,<<"file">>},
%%               {<<"value">>,<<"File">>},
%%               {<<"popup">>,
%%                  {[{<<"menu_item">>,
%%                     [{[{<<"value">>,<<"New">>},
%%                        {<<"onclick">>,<<"CreateNewDoc()">>}]},
%%                        {[{<<"onclick">>,<<"OpenDoc()">>},
%%                           {<<"val">>,<<"Open">>}]},
%%                        {[{<<"value">>,<<"Close">>},
%%                           {<<"onclick">>,<<"CloseDoc()">>},
%%                           {<<"ondbclick">>,<<"print()">>}]}]}]}}]}}]}},
%%         {<<"var">>,44}]
%%   ).

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
         <<"vs">> => 1,
         <<"df">> => <<"01.002">>,
         <<"data">> => #{<<"value1">> => 323424, <<"value2">> => <<"somestringvalue">>}}

   ).

json_basic_vs_test() ->
   P = #data_point{ts = 1568029511598, fields = #{<<"id">> => <<"ioi2u34oiu23oi4u2oi4u2">>,
      <<"df">> => <<"01.002">>, <<"vs">> => 2, <<"value1">> => 323424,
      <<"value2">> => <<"somestringvalue">>}},
   ?assertEqual(jiffy:decode(to_json(P), [return_maps]),
      #{<<"ts">> =>1568029511598,
         <<"id">> => <<"ioi2u34oiu23oi4u2oi4u2">>,
         <<"vs">> => 2,
         <<"df">> => <<"01.002">>,
         <<"data">> => #{<<"value1">> => 323424, <<"value2">> => <<"somestringvalue">>}}

   ).

json_basic_default_test() ->
   P = #data_point{ts = 1568029511598,
      fields = #{<<"value1">> => 323424, <<"value2">> => <<"somestringvalue">>}},
   ?assertEqual(jiffy:decode(to_json(P), [return_maps]),
      #{<<"ts">> =>1568029511598,
         <<"id">> => <<"00000">>,
         <<"vs">> => 1,
         <<"df">> => <<"00.000">>,
         <<"data">> => #{<<"value1">> => 323424, <<"value2">> => <<"somestringvalue">>}}
   ).

json_basic_data_test() ->
   P = #data_point{ts = 1568029511598, fields =
   #{<<"id">> => <<"ioi2u34oiu23oi4u2oi4u2">>,
      <<"df">> => <<"01.002">>, <<"vs">> => 2,
      <<"data">> => #{<<"value1">> => 323424, <<"value2">> => <<"somestringvalue">>}}
   },

   ?assertEqual(jiffy:decode(to_json(P), [return_maps]),

      #{<<"ts">> =>1568029511598,
         <<"id">> => <<"ioi2u34oiu23oi4u2oi4u2">>,
         <<"vs">> => 2,
         <<"df">> => <<"01.002">>,
         <<"data">> => #{<<"value1">> => 323424, <<"value2">> => <<"somestringvalue">>}}


   ).

json_basic_data_excl_test() ->
   P = #data_point{ts = 1568029511598, fields =
      #{<<"id">> => <<"ioi2u34oiu23oi4u2oi4u2">>,
         <<"df">> => <<"01.002">>, <<"vs">> => 2, <<"value1">> => 2323422, <<"value2">> => <<"savoi">>,
         <<"data">> => #{<<"value1">> => 323424, <<"value2">> => <<"somestringvalue">>}}
   },
   ?assertEqual(jiffy:decode(to_json(P), [return_maps]),
      #{<<"ts">> =>1568029511598,
         <<"id">> => <<"ioi2u34oiu23oi4u2oi4u2">>,
         <<"vs">> => 2,
         <<"df">> => <<"01.002">>,
         <<"data">> => #{<<"value1">> => 323424, <<"value2">> => <<"somestringvalue">>}}

   ).


batch_to_json_test() ->
   P = #data_point{ts = 1568029511598, fields =
   #{<<"id">> => <<"ioi2u34oiu23oi4u2oi4u2">>,
      <<"df">> => <<"01.002">>, <<"vs">> => 2, <<"value1">> => 2323422, <<"value2">> => <<"savoi">>,
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
         <<"ts">> => 1568029512598,<<"vs">> => 2},
         #{<<"data">> =>
         #{<<"value1">> => 323424,
            <<"value2">> => <<"somestringvalue">>},
            <<"df">> => <<"01.002">>,
            <<"id">> => <<"ioi2u34oiu23oi4u2oi4u2">>,
            <<"ts">> => 1568029513598,<<"vs">> => 2},
         #{<<"data">> =>
         #{<<"value1">> => 323424,
            <<"value2">> => <<"somestringvalue">>},
            <<"df">> => <<"01.002">>,
            <<"id">> => <<"ioi2u34oiu23oi4u2oi4u2">>,
            <<"ts">> => 1568029514598,<<"vs">> => 2},
         #{<<"data">> =>
         #{<<"value1">> => 323424,
            <<"value2">> => <<"somestringvalue">>},
            <<"df">> => <<"01.002">>,
            <<"id">> => <<"ioi2u34oiu23oi4u2oi4u2">>,
            <<"ts">> => 1568029515598,<<"vs">> => 2},
         #{<<"data">> =>
         #{<<"value1">> => 323424,
            <<"value2">> => <<"somestringvalue">>},
            <<"df">> => <<"01.002">>,
            <<"id">> => <<"ioi2u34oiu23oi4u2oi4u2">>,
            <<"ts">> => 1568029516598,<<"vs">> => 2}]

   ).

msgpack_basic_test() ->
   P = #data_point{ts = 1568029511598, fields = #{<<"id">> => <<"ioi2u34oiu23oi4u2oi4u2">>,
      <<"df">> => <<"01.002">>, <<"vs">> => 2,
      <<"data">> => #{<<"value1">> => 323424, <<"value2">> => <<"somestringvalue">>}}
   },
   ?assertEqual(msgpack:unpack(flowdata:to_s_msgpack(P),[{map_format,map}]),
      {ok,#{<<"ts">> =>1568029511598,
         <<"id">> => <<"ioi2u34oiu23oi4u2oi4u2">>,
         <<"vs">> => 2,
         <<"df">> => <<"01.002">>,
         <<"data">> => #{<<"value1">> => 323424, <<"value2">> => <<"somestringvalue">>}}
      }
   ).

-endif.
