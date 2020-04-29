%% Date: 30.12.16 - 23:01
%% Query CrateDB, time series data
%% Ⓒ 2019 heyoka
%%
-module(esp_crate_query).
-author("Alexander Minichmair").

-include("faxe.hrl").

-behavior(df_component).
%% API
-export([init/3, process/3, options/0, handle_info/2, to_flowdata/2, check_options/0, shutdown/1]).

-record(state, {
   host :: string(),
   port :: non_neg_integer(),
   query :: iodata(),
   user :: string(),
   pass :: string(),
   database :: iodata(),
   client,
   client_ref,
   stmt,
   db_opts,
   every,
   period,
   result_type,
   align = false,
   timer
}).

-define(DB_OPTIONS, #{
   codecs => [{faxe_epgsql_codec, nil}, {epgsql_codec_json, {jiffy, [], [return_maps]}}],
   timeout => 3000
}).

-define(STMT, "stmt").


options() ->
   [
      {host, string, {crate, host}},
      {port, integer, {crate, port}},
      {user, string, {crate, user}},
      {pass, string, <<>>},
      {database, string, {crate, database}},
      {query, string},
      {time_field, string, <<"ts">>},
      {every, duration, <<"5s">>},
      {period, duration, <<"1h">>},
      {align, is_set},
      {group_by_time, duration, <<"2m">>},
      {group_by, string_list, []},
      {limit, string, <<"30">>},
      {result_type, string, <<"batch">>}
   ].

check_options() ->
   [
      {func, query, fun faxe_util:check_select_statement/1, <<"seems not to be a valid sql select statement">>}
   ].

init(_NodeId, _Inputs, #{host := Host0, port := Port, user := User, every := Every, period := Period,
      pass := Pass, database := DB, query := Q0, align := Align, group_by_time := TimeGroup,
      time_field := TimeField, group_by := GroupBys, result_type := RType}) ->

   process_flag(trap_exit, true),
   Host = binary_to_list(Host0),
   Opts = #{host => Host, port => Port, username => User, pass => Pass, database => DB},
   DBOpts = maps:merge(?DB_OPTIONS, Opts),

%%   lager:warning("the QUERY before: ~p",[Q0]),
   Q = faxe_util:clean_query(Q0),
   Query = build_query(Q, TimeGroup, TimeField, GroupBys),
%%   lager:warning("the QUERY: ~p",[Query]),
   State = #state{host = Host, port = Port, user = User, pass = Pass, database = DB, query = Query,
      db_opts = DBOpts, every = Every, period = faxe_time:duration_to_ms(Period),
      align = Align, result_type = RType},
   erlang:send_after(0, self(), reconnect),
   {ok, all, State}.

process(_In, _P = #data_point{}, State = #state{}) ->
   {ok, State};
process(_In, _B = #data_batch{}, State = #state{}) ->
   {ok, State}.


handle_info(query,
    State = #state{timer = Timer, client = C, stmt = _Q, period = Period, result_type = RType}) ->
   QueryMark = Timer#faxe_timer.last_time,
%%   lager:notice("query: ~p with ~p from: ~p, to :~p",
%%      [Q, [QueryMark-Period, QueryMark], faxe_time:to_iso8601(QueryMark-Period), faxe_time:to_iso8601(QueryMark)]),
   NewTimer = faxe_time:timer_next(Timer),
   %% do query
   Resp = epgsql:prepared_query(C, ?STMT, [QueryMark-Period, QueryMark]),
   handle_response(Resp, RType, State#state{timer = NewTimer});

handle_info({'EXIT', _C, Reason}, State = #state{timer = Timer}) ->
   lager:warning("EXIT epgsql with reason: ~p",[Reason]),
   NewTimer = cancel_timer(Timer),
   NewState = connect(State),
   {ok, NewState#state{timer = NewTimer}};
handle_info(reconnect, State) ->
   {ok, connect(State)};
handle_info(What, State) ->
   lager:warning("++other info : ~p",[What]),
   {ok, State}.

shutdown(#state{client = C, stmt = _Stmt}) ->
   catch epgsql:close(C).

connect(State = #state{db_opts = Opts, query = Q}) ->
   lager:info("db opts: ~p",[Opts]),
   case epgsql:connect(Opts) of
      {ok, C} ->
         case epgsql:parse(C, ?STMT, Q, [int8, int8]) of
            {ok, Statement} ->
               NewState = init_timer(State#state{client = C, stmt = Statement}),
               NewState;
            Other ->
               lager:error("Can not parse prepared statement: ~p",[Other]),
               error("parsing prepared statement failed!"),
               State
         end;
      {error, What} ->
         lager:warning("Error connecting to crate: ~p",[What]),
         State
   end.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
build_query(<<"SELECT", Query/binary>>, TimeGroup, TimeField, GroupBys) ->
   GroupTimeStatement = time_group(TimeGroup, TimeField),
   GroupByClause = build_group_bys(GroupBys),
   TimeRangeClause = time_range(TimeField, Query),
   <<
      "SELECT ", GroupTimeStatement/binary, ", ", (string:trim(Query))/binary, TimeRangeClause/binary,
      " GROUP BY ", TimeField/binary, "_gb",
      GroupByClause/binary, " ORDER BY ", TimeField/binary, "_gb DESC"
   >>.

time_group(GroupTimeOption, TimeField) ->
   Dur0 = round(faxe_time:duration_to_ms(GroupTimeOption)/1000),
   Dur = list_to_binary(integer_to_list(Dur0)),
   <<
      "floor(EXTRACT(epoch FROM ", TimeField/binary, ")/", Dur/binary, ")*", Dur/binary, " AS ",
      TimeField/binary, "_gb"
   >>.

time_range(TimeField, Query) ->
   B0 =
      case binary:match(Query, <<"WHERE">>) of
         nomatch -> <<" WHERE ">>;
         _        -> <<" AND ">>
      end,
   << B0/binary, TimeField/binary, " >= $1 AND ", TimeField/binary, " <= $2" >>
.

build_group_bys(GroupByList) ->
   build_group_bys(GroupByList, <<>>).
build_group_bys([], GroupClause) ->
   GroupClause;
build_group_bys([GroupField|R], GroupClause) ->
   Acc = <<GroupClause/binary, ", ", GroupField/binary>>,
   build_group_bys(R, Acc).

init_timer(S = #state{align = Align, every = Every}) ->
   Timer = faxe_time:init_timer(Align, Every, query),
   S#state{timer = Timer}.

%% result handling

handle_response({ok, Columns, Rows}, ResponseType, State) ->
   ColumnNames = columns(Columns, []),
   Batch = handle_result(ColumnNames, Rows, ResponseType),
   {emit, {1, Batch}, State};
handle_response(Other, _RType, State) ->
   lager:warning("Response from Crate: ", [Other]),
   {ok, State}.


columns([], ColumnNames) ->
   lists:reverse(ColumnNames);
columns([{column, Name, _Type, _, _, _, _}|RestC], ColumnNames) ->
   columns(RestC, [Name|ColumnNames]).

handle_result(Columns, Rows, <<"batch">>) ->
   to_flowdata(Columns, Rows);
handle_result(Columns, Rows, <<"point">>) ->
   Batch = to_flowdata(Columns, Rows),
   FieldsList = [Fields || #data_point{fields = Fields} <- Batch#data_batch.points],
   #data_point{ts = faxe_time:now(), fields = #{<<"data">> => FieldsList}}.



to_flowdata(Columns, ValueRows) ->
   VRows = [tuple_to_list(VRow) || VRow <- ValueRows],
   to_flowdata(Columns, lists:reverse(VRows), #data_batch{}).

to_flowdata(_C, [], Batch=#data_batch{}) ->
   Batch;
to_flowdata([<<"ts_gb">>|Columns]=C, [[Ts|ValRow]|Values], Batch=#data_batch{points = Points}) ->
   Point = row_to_datapoint(Columns, ValRow,
      #data_point{ts = faxe_epgsql_codec:decode(Ts, timestamp, nil)}),
   to_flowdata(C, Values, Batch#data_batch{points = [Point|Points]}).

row_to_datapoint([], [], Point) ->
   Point;
row_to_datapoint([C|Columns], [Val|Row], Point) ->
   P = flowdata:set_field(Point, C, Val),
   row_to_datapoint(Columns, Row, P).

-spec cancel_timer(#faxe_timer{}|undefined) -> #faxe_timer{}|undefined.
cancel_timer(Timer) ->
   case catch (faxe_time:timer_cancel(Timer)) of
      T = #faxe_timer{} -> T;
      _ -> Timer
   end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% TESTS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-ifdef(TEST).
time_group_test() ->
   Expected = <<"floor(EXTRACT(epoch FROM ts)/420)*420 AS ts_gb">>,
   ?assertEqual(Expected,time_group(<<"7m">>, <<"ts">>)).

build_simple_query_test() ->
   Expected = <<"SELECT floor(EXTRACT(epoch FROM time)/300)*300 AS time_gb, COUNT(*) FROM table ",
   "WHERE tag1 = 'test' AND time >= $1 AND time <= $2 GROUP BY time_gb, a, b ORDER BY time_gb DESC">>,
   Query = <<"SELECT COUNT(*) FROM table WHERE tag1 = 'test'">>,
   ?assertEqual(Expected, build_query(Query, <<"5m">>, <<"time">>, [<<"a">>,<<"b">>])).
-endif.