%%%-------------------------------------------------------------------
%%% @author heyoka
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 30. Mar 2021 08:22
%%%-------------------------------------------------------------------
-module(df_graph_test).
-author("heyoka").

-include_lib("eunit/include/eunit.hrl").

%% API
-export([check_select_with_test/0]).

check_select_with_test() ->
  Sql = <<"with \"task\" as (
    SELECT
      ts as \"tsTask\",
      {{ws_task_dbcol}}[''quantity''] as \"quantity\",
      {{ws_task_dbcol}}[''sourceSectionName''] as \"crateName\",
      {{ws_task_dbcol}}[''sourcelcaName''] AS \"lcaName\",
      {{ws_task_dbcol}}[''sku''] as \"sku\"
    FROM {{dest_schema}}.{{table}}
    where
      $__timefilter AND
      {{ws_task_dbcol}}[''quantity''] > 0 AND
      stream_id in ( {{ws_task_db_sid}} )
    )

  select
    \"task\".\"tsTask\" as \"ts\",
    {{crate_dbcol}}[''crateName''] as \"crateName\",
    {{crate_dbcol}}[''lcaName''] as \"lcaName\",
    array_max([{{crate_dbcol}}[''quantity''] - \"task\".\"quantity\", 0]) as \"quantity\",
    if( {{crate_dbcol}}[''quantity''] - \"task\".\"quantity\" <= 0, true, false) as \"isEmpty\",
    {{crate_dbcol}}[''sku''] as \"sku\",
    if( {{crate_dbcol}}[''quantity''] - \"task\".\"quantity\" <= 0, ''Empty'', ''Broken'') as \"status\",
    ''None'' as \"reason\"
  FROM {{dest_schema}}.{{table}}, \"task\",
    (
    SELECT
      max(ts) as \"tsMax\",
      {{crate_dbcol}}[''lcaName''] as \"lcaName\"
    FROM {{dest_schema}}.{{table}}, \"task\"
    WHERE
      {{crate_dbcol}}[''lcaName''] = \"task\".\"lcaName\" and
      ts <= \"task\".\"tsTask\" and
      stream_id in ( {{crate_db_sid}}, {{crate_input_db_sid}} )
    GROUP By 2
    ) as \"tsMax\"
  where
    {{crate_dbcol}}[''lcaName''] = \"task\".\"lcaName\" and
    {{crate_dbcol}}[''lcaName''] = \"tsMax\".\"lcaName\" and
    ts = \"tsMax\".\"tsMax\" and
    stream_id in ( {{crate_db_sid}}, {{crate_input_db_sid}} )">>,
  ?assert(faxe_util:check_select_statement(Sql)).


check_select_with_1_test() ->
  Sql = <<"with \"task\"
  select
    \"task\".\"tsTask\" as \"ts\",
    {{crate_dbcol}}[''crateName''] as \"crateName\",
    {{crate_dbcol}}[''lcaName''] as \"lcaName\",
    array_max([{{crate_dbcol}}[''quantity''] - \"task\".\"quantity\", 0]) as \"quantity\",
    if( {{crate_dbcol}}[''quantity''] - \"task\".\"quantity\" <= 0, true, false) as \"isEmpty\",
    {{crate_dbcol}}[''sku''] as \"sku\",
    if( {{crate_dbcol}}[''quantity''] - \"task\".\"quantity\" <= 0, ''Empty'', ''Broken'') as \"status\",
    ''None'' as \"reason\"
  FROM {{dest_schema}}.{{table}}, \"task\",
    (
    SELECT
      max(ts) as \"tsMax\",
      {{crate_dbcol}}[''lcaName''] as \"lcaName\"
    FROM {{dest_schema}}.{{table}}, \"task\"
    WHERE
      {{crate_dbcol}}[''lcaName''] = \"task\".\"lcaName\" and
      ts <= \"task\".\"tsTask\" and
      stream_id in ( {{crate_db_sid}}, {{crate_input_db_sid}} )
    GROUP By 2
    ) as \"tsMax\"
  where
    {{crate_dbcol}}[''lcaName''] = \"task\".\"lcaName\" and
    {{crate_dbcol}}[''lcaName''] = \"tsMax\".\"lcaName\" and
    ts = \"tsMax\".\"tsMax\" and
    stream_id in ( {{crate_db_sid}}, {{crate_input_db_sid}} )">>,
  ?assert(faxe_util:check_select_statement(Sql)).

check_select_with_no_from_test() ->
  Sql = <<"with \"task\" as
  select
    \"task\".\"tsTask\" as \"ts\",
    {{crate_dbcol}}[''crateName''] as \"crateName\",
    {{crate_dbcol}}[''lcaName''] as \"lcaName\",
    array_max([{{crate_dbcol}}[''quantity''] - \"task\".\"quantity\", 0]) as \"quantity\",
    if( {{crate_dbcol}}[''quantity''] - \"task\".\"quantity\" <= 0, true, false) as \"isEmpty\",
    {{crate_dbcol}}[''sku''] as \"sku\",
    if( {{crate_dbcol}}[''quantity''] - \"task\".\"quantity\" <= 0, ''Empty'', ''Broken'') as \"status\",
    ''None'' as \"reason\"">>,
  ?assertEqual(false, faxe_util:check_select_statement(Sql)).
