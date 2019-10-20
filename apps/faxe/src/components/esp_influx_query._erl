%% Date: 05.01.17 - 10:21
%% Ⓒ 2017 heyoka
%% @doc
%% query influxdb
%% @end
-module(esp_influx_query).
-author("Alexander Minichmair").

-behaviour(df_component).

-include("faxe.hrl").
%% API
-export([init/3, process/3, handle_info/2, options/0, handle_result/1, test_start/0, test/0, test_build/0]).

-define(DB,             <<"lm3">>).
-define(MEASUREMENT,    <<"m1">>).

-define(FILL_DEFAULT, <<"null">>).

-define(GROUP_BY, <<"GROUP BY">>).

% SHOW TAG VALUES FROM m1 WITH KEY IN (\"stream\")


-record(state, {
   node_id           :: term(),
   query             :: binary(),
   group_time        :: binary(),
   group_tags        :: list(),
   period            :: non_neg_integer(), %% query timerange in milliseconds
   every             :: non_neg_integer(),
   cron              :: binary(),
   align             :: binary(),
   offset            :: binary(),
   fill              :: binary()|term(),

   mark              :: non_neg_integer() %% query from this timestamp back in time, "period" milliseconds
}).

%% test it %%
-define(Q, <<"SELECT mean(val) FROM m1 WHERE stream='1.002.eb195daeff594de58a0eaee88cf1190b'">>).
%% fluxer:query_epoch_ms(<<"lm3">>,<<"SELECT mean(val) FROM m1
%% WHERE stream='1.002.eb195daeff594de58a0eaee88cf1190b' AND time > now() - 3h GROUP BY time(15m)">>).
test() ->
   handle_result(
   fluxer:query_epoch_ms(?DB,
      << <<"SELECT mean(val) as avg FROM m1 WHERE">>/binary,
   <<" stream='1.002.eb195daeff594de58a0eaee88cf1190b' AND time > now() - 3h GROUP BY time(15m)">>/binary >>)
   ).

%%%%%%%%%%%%%%%%
test_build() ->
   lager:info("NOW: ~p",[faxe_time:now_date()]),
   S = #state{node_id = <<"influx_query">>, query = ?Q, period = 7*60*60*1000, every = 30,
      fill = <<"previous">>, group_tags = [<<"stream">>], group_time = <<"45m">>, offset = 0,
      align = true},
   State = next_mark(S),
   UMark = next_mark(State),
   lager:notice("ubernext mark at: ~p",[faxe_time:to_date(UMark#state.mark)]),
   lager:notice("next mark at: ~p",[faxe_time:to_date(State#state.mark)]),
   query(State).
%%   lager:info("QUERY: ~p",[build_query(S)]).

test_start() ->
   Opts = #{query => ?Q, group_by_time => <<"15m">>,
      period => 3*60*60*1000, every => 30*60*1000,
      offset => 0, fill => <<"none">>},
   df_component:start_link(?MODULE, <<"influx_query">>, 1, 1, maps:to_list(Opts)).

options() ->
   [
      {query,           binary},
      %% groupBy(time(1m))
      {group_by_time,   duration},
      %% groupBy('tag1', 'tag2')
      {group_by_tags,   list},
      %% period(10s)
      {period,          duration},
      %% every(30s)
      {every,           duration},
      %% cron("001000")
      {cron,            binary},
      %% align()
      {align,           is_set},
      %% offset(-12h)
      {offset,          duration},
      %% fill(none|null|{value}|linear|previous)
      {fill,            any}
   ].

init(NodeId, _Inputs, #{query := Q, group_by_time := GroupTime, group_by_tags := GroupTags,
      period := Period, every := Every, cron := Cron, align := Align, offset := Off, fill := Fill}) ->

   State0 = #state{node_id = NodeId, query = Q, period = Period, every = Every, align = Align,
      fill = Fill, cron = Cron, group_tags = GroupTags, group_time = GroupTime, offset = Off},

   State = next_mark(State0),

   {ok, none, State}.


process(_Inport, Value, State) ->
   io:format("~p process, ~p~n",[State, {_Inport, Value}]),
   {ok, State}.


handle_info(query, State=#state{every = Every}) ->
   io:format("~nrequest: ~p~n", [query(State)]),
   NewState = next_mark(State),
%%   erlang:send_after(Every, self(), query),
   {ok, NewState}.



%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Internal
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
next_mark(S=#state{align = Align, every = Every, mark = undefined}) ->
   Now = faxe_time:now(),
   NewTs1 =
   case Align of
      true -> faxe_time:align(Now, {minute, Every});
      false -> Now
   end,
   next_mark(S#state{mark = NewTs1});
next_mark(S=#state{every = Every, mark = Mark}) ->
   NewTs = faxe_time:add_unit(Mark, minute, Every),
   lager:info("send in : ~p",[faxe_time:to_date(NewTs - faxe_time:now())]),
   erlang:send_after(NewTs - faxe_time:now(), self(), query),
   S#state{mark = NewTs}.

query(State = #state{}) ->
   Q = build_query(State),
   lager:notice("Query: ~p", [Q]),
   query(Q);
query(Query) when is_binary(Query) ->
   Result = fluxer:query_epoch_ms(?DB, Query),
   handle_result(Result).



%% SHOW TAG VALUES
handle_result(
    {ok,[{<<"results">>,
   [[{<<"series">>,
      [[{<<"name">>, _SeriesName},
         {<<"columns">>,[<<"key">>,<<"value">>]},
         {<<"values">>,Values}]]}]]}]}
) ->
   {ok, Values};
%% SELECT values
handle_result(
    {ok,[{<<"results">>,
   [[{<<"series">>,
      [[{<<"name">>, _SeriesName},
         {<<"columns">>, Columns}, %%[<<"time">>,<<"mean">>]},
         {<<"values">>, Values}]]}]]}]}
) ->
   lager:info("~p ",[_SeriesName]),
   {ok, to_flowdata(Columns, Values)};
handle_result(
    {ok,[{<<"results">>,
       [[{<<"series">>,
          [[{<<"name">>, _SeriesName},
             {<<"tags">>, Tags},
             {<<"columns">>, Columns}, %%[<<"time">>,<<"mean">>]},
             {<<"values">>, Values}]]}]]}]}
) ->
   lager:info("~p -> Returned Tags: ~p",[_SeriesName, Tags]),
   {ok, to_flowdata(Columns, Values)};
%% empty result
handle_result({ok,[{<<"results">>,[[{}]]} ]}) -> {ok, []};
%% wrong database // <<"database not found: lm33">>
handle_result({ok,[{<<"results">>, [[{<<"error">>, ErrorBin}]]}]}) -> {error, ErrorBin};
%% other response
handle_result(Result) -> eval_response(Result).
-spec eval_response(tuple()|any()) -> 'failed' | {'error', any()}.
%% query error -> 400
eval_response({ok, {{<<"4", _R:2/binary>>, _}, _Hdrs, _Resp, _, _}}) -> {error, _Resp};
eval_response({ok, {{<<"503">>, _}, _Hdrs, _Resp, _, _}})            -> {not_avaiable, _Resp};
eval_response({ok, {{<<"5", _R:2/binary>>, _}, _Hdrs, _Resp, _, _}}) -> {failed, _Resp};
eval_response({'EXIT',Reason})                                       -> {not_avaiable, Reason};
eval_response(_What)                                                 -> {not_avaiable, _What}.



to_flowdata(Columns, ValueRows) ->
   to_flowdata(Columns, lists:reverse(ValueRows), #data_batch{}).

to_flowdata(_C, [], Batch=#data_batch{}) ->
   Batch;
to_flowdata([<<"time">>|Columns]=C, [[Ts|ValRow]|Values], Batch=#data_batch{points = Points}) ->
   Point = row_to_datapoint(Columns, ValRow, #data_point{ts = Ts, id = faxe_time:to_date(Ts)}),
   to_flowdata(C, Values, Batch#data_batch{points = [Point|Points]}).

row_to_datapoint([], [], Point) ->
   Point;
row_to_datapoint([C|Columns], [Val|Row], Point) ->
   P = flowdata:set_field(Point, C, Val),
   row_to_datapoint(Columns, Row, P).



%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% query building
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%<<"SELECT mean(val) FROM m1 WHERE stream=1.002.3124lh123j4h123j4l123l4">>).
build_query(S=#state{group_tags = GTags, group_time = GTime, query = Q,
      fill = Fill, period = Period, mark = QueryMark}) ->

   GroupByClause =
   case {GTags, GTime} of
      {undefined, undefined} ->
         <<>>; %% no grouping -> no group by clause
      {Tags, undefined} ->
         TagsBin = tags_to_bin_string(Tags),
         << ?GROUP_BY/binary, TagsBin/binary>>;
      {undefined, GTime} ->
         B = << ?GROUP_BY/binary, (time_group(GTime))/binary>>,
         case Fill of
            undefined -> B;
            FillOption -> <<B/binary, (fill(FillOption))/binary >>
         end;
      {GTags, GTime} ->
         TagsBin = tags_to_bin_string(GTags),
         B = << ?GROUP_BY/binary, (time_group(GTime))/binary, <<",">>/binary, TagsBin/binary>>,
         case Fill of
            undefined -> B;
            FillOption -> <<B/binary, (fill(FillOption))/binary>>
         end
   end,

   <<Q/binary, (time_range(QueryMark, Period, Q))/binary, GroupByClause/binary>>.



tags_to_bin_string(Tags) when is_list(Tags) ->
   lists:foldl(
      fun(El, <<>>) -> <<El/binary>>;
         (El, Bin) -> <<Bin/binary, <<",">>/binary, El/binary>>
      end,
      <<>>,
      Tags
   ).

time_group(TimeOption) ->
   <<
      <<" time(">>/binary, TimeOption/binary, <<")">>/binary
   >>.

fill(?FILL_DEFAULT) ->
      <<"">>;
fill(FillOption) ->
   << <<" fill(">>/binary, FillOption/binary, <<")">>/binary >>.

time_range(QueryMark, Period, Query) ->
   lager:info("TIME_RANGE: ~p :: ~p",[faxe_time:to_date(QueryMark-Period), faxe_time:to_date(QueryMark)]),
   B0 =
   case binary:match(Query, <<"WHERE">>) of
      nomatch -> <<" WHERE">>;
      _        -> <<" AND">>
   end,
   << B0/binary,
      <<" time >= ">>/binary, (integer_to_binary(QueryMark-Period))/binary,
      <<"ms AND time <= ">>/binary, (integer_to_binary(QueryMark))/binary,
      <<"ms ">>/binary
   >>
.
