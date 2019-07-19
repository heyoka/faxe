%% Date: 23.12.16 - 19:28
%% Ⓒ 2016 heyoka
-module(win_test).
-author("Alexander Minichmair").

%% API
-export([]).

-compile(export_all).

stream() ->
   N1 = "stream_in", N3 = "stream_out",
   {ok, Graph} = df_graph:start_link("graph1",[]),
   StreamOpts = [{stream_ids, [<<"1.002.eb195daeff594de58a0eaee88cf1190b">>]}, {key, <<"esp_stream_test">>}, {prefetch, 1}],
   df_graph:add_node(Graph, N1, esp_stream_in, StreamOpts),

   OutOpts = [{stream_id, <<"1.023.eb195dae89sduf9s8udf88cf1190b">>}],
   df_graph:add_node(Graph, N3, esp_stream_out, OutOpts),

   df_graph:add_edge(Graph, N1, 1, N3, 1, []),
%%   Graph ! info,
   df_graph:start_graph(Graph, push).


stream_aggregation() ->
   N1 = "value_producer", N2 = "aggregator",
   {ok, Graph} = df_graph:start_link("graph1",[]),

   StreamOpts = [{every, 1000*5},{type, batch}, {batch_size, 3}, {align, minute}],
   df_graph:add_node(Graph, N1, esp_value_emitter, StreamOpts),

   AggOpts = [{module, esp_min},{field, <<"val">>},{as, <<"avg">>}],
   df_graph:add_node(Graph, N2, esp_stats, AggOpts),

%%   OutOpts = [{stream_id, <<"1.023.eb195dae89sduf9s8udf88cf1190b">>}],
%%   df_graph:add_node(Graph, N3, esp_stream_out, OutOpts),

   df_graph:add_edge(Graph, N1, 1, N2, 1, []),
%%   df_graph:add_edge(Graph, N2, 1, N3, 1, []),
   df_graph:start_graph(Graph, push).

shift() ->
   N1 = "value_producer", N2 = "aggregator",
   {ok, Graph} = df_graph:start_link("graph1",[]),

   StreamOpts = [{every, 1000*5},{type, batch}],
   df_graph:add_node(Graph, N1, esp_value_emitter, StreamOpts),

   ShiftOpts = [{unit, second},{interval, -1}],
   df_graph:add_node(Graph, N2, esp_shift, ShiftOpts),

%%   OutOpts = [{stream_id, <<"1.023.eb195dae89sduf9s8udf88cf1190b">>}],
%%   df_graph:add_node(Graph, N3, esp_stream_out, OutOpts),

   df_graph:add_edge(Graph, N1, 1, N2, 1, []),
%%   df_graph:add_edge(Graph, N2, 1, N3, 1, []),
   df_graph:start_graph(Graph, push).


stream_lambda() ->
   N1 = "stream_in", N2 = "lambda1", N3 = "stream_out",
   {ok, Graph} = df_graph:start_link("graph1",[]),

   StreamOpts = [{stream_ids, [<<"1.002.eb195daeff594de58a0eaee88cf1190b">>]}, {key, <<"esp_stream_test">>}, {prefetch, 1}],
   df_graph:add_node(Graph, N1, esp_stream_in, StreamOpts),

   LambdaOpts = [],
   df_graph:add_node(Graph, N2, esp_erl_lambda, LambdaOpts),

   OutOpts = [{stream_id, <<"1.023.eb195dae89sduf9s8udf88cf1190b">>}],
   df_graph:add_node(Graph, N3, esp_stream_out, OutOpts),

   df_graph:add_edge(Graph, N1, 1, N2, 1, []),
   df_graph:add_edge(Graph, N2, 1, N3, 1, []),
   df_graph:start_graph(Graph, push).

stream_win_event() ->
   N1 = "stream_in", N2 = "printer", N3 = "stream_out",
   {ok, Graph} = df_graph:start_link("graph1",[]),
   StreamOpts = [{stream_ids, [<<"1.002.eb195daeff594de58a0eaee88cf1190b">>]}, {key, <<"esp_stream_test">>}, {prefetch, 1}],
   df_graph:add_node(Graph, N1, esp_stream_in, StreamOpts),

   WinOpts = [{period, 5}, {every, 5}],
   df_graph:add_node(Graph, N2, esp_win_event, WinOpts),

   OutOpts = [{stream_id, <<"1.023.eb195dae89sduf9s8udf88cf1190b">>}],
   df_graph:add_node(Graph, N3, esp_stream_out, OutOpts),

   df_graph:add_edge(Graph, N1, 1, N3, 1, []),
   df_graph:add_edge(Graph, N1, 1, N2, 1, []),
%%   Graph ! info,
   df_graph:start_graph(Graph, push).

stream_win_time() ->
   N1 = "stream_in", N2 = "printer", N3 = "stream_out",
   {ok, Graph} = df_graph:start_link("graph1",[]),
   StreamOpts = [{stream_ids, [<<"1.002.eb195daeff594de58a0eaee88cf1190b">>]}, {key, <<"esp_stream_test">>}, {prefetch, 1}],
   df_graph:add_node(Graph, N1, esp_stream_in, StreamOpts),

   WinOpts = [{period, 2*1000*60}, {every, 1*1000*60}],
   df_graph:add_node(Graph, N2, esp_win_time, WinOpts),

   OutOpts = [{stream_id, <<"1.023.eb195dae89sduf9s8udf88cf1190b">>}],
   df_graph:add_node(Graph, N3, esp_stream_out, OutOpts),

   df_graph:add_edge(Graph, N1, 1, N3, 1, []),
   df_graph:add_edge(Graph, N1, 1, N2, 1, []),
%%   Graph ! info,
   df_graph:start_graph(Graph, push).

stream_win_clock() ->
   N1 = "stream_in", N2 = "printer", N3 = "stream_out",
   {ok, Graph} = df_graph:start_link("graph1",[]),
   StreamOpts = [{stream_ids, [<<"1.002.eb195daeff594de58a0eaee88cf1190b">>]}, {key, <<"esp_stream_test">>}, {prefetch, 1}],
   df_graph:add_node(Graph, N1, esp_stream_in, StreamOpts),

   WinOpts = [{period, 2*1000*60}, {every, 1*1000*60}],
   df_graph:add_node(Graph, N2, esp_win_clock, WinOpts),

%%   OutOpts = [{stream_id, <<"1.023.eb195dae89sduf9s8udf88cf1190b">>}],
%%   df_graph:add_node(Graph, N3, esp_stream_out, OutOpts),

%%   df_graph:add_edge(Graph, N1, 1, N3, 1, []),
   df_graph:add_edge(Graph, N1, 1, N2, 1, []),
%%   Graph ! info,
   df_graph:start_graph(Graph, push).

time() ->
   esp_window_time:start_link(self(), 1000*60*15, 3*60*1000, esp_agg_noop_values).

clock() ->
   esp_window_wallclock:start_link(self(), 30000, 5000, esp_stats_avg).

event() ->
   esp_window_event:start_link(self(), 4, 4, esp_agg_noop_values).