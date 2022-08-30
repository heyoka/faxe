%% Date: 30.12.16 - 23:01
%% Query CrateDB, time series data
%% Ⓒ 2019 heyoka
%%
-module(esp_crate_query).
-author("Alexander Minichmair").

-include("faxe.hrl").
-include("faxe_epgsql_response.hrl").

-behavior(df_component).
%% API
-export([init/3, process/3, options/0, handle_info/2, check_options/0, shutdown/1, metrics/0]).

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
   timer,
   resp_def :: faxe_epgsql_response(),
   fn_id,
   debug_mode = false
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
      {one_of, result_type, [<<"batch">>, <<"point">>]},
      {func, query, fun faxe_util:check_select_statement/1, <<"seems not to be a valid sql select statement">>}
   ].

metrics() ->
   [
      {?METRIC_BYTES_READ, meter, []},
      {?METRIC_READING_TIME, histogram, [slide, 30], "The number of milliseconds it took the query-result to return."}
   ].

init(NodeId, _Inputs, #{host := Host0, port := Port, user := User, every := Every, period := Period,
      pass := Pass, database := DB, query := Q0, align := Align, group_by_time := TimeGroup,
      time_field := TimeField, group_by := GroupBys, result_type := RType}) ->

   process_flag(trap_exit, true),
   Host = binary_to_list(Host0),
   Opts = #{host => Host, port => Port, username => User, password => Pass, database => DB},
   DBOpts = maps:merge(?DB_OPTIONS, Opts),

%%   lager:warning("the QUERY before: ~p",[Q0]),
   Q = faxe_util:clean_query(Q0),
   Query = build_query(Q, TimeGroup, TimeField, GroupBys),
%%   lager:warning("the QUERY: ~p",[Query]),
   Response = faxe_epgsql_response:new(TimeField, erlang:binary_to_existing_atom(RType), <<"data">>),
   State = #state{host = Host, port = Port, user = User, pass = Pass, database = DB, query = Query,
      db_opts = DBOpts, every = Every, period = faxe_time:duration_to_ms(Period), resp_def = Response,
      align = Align, result_type = RType, fn_id = NodeId},
   connection_registry:reg(NodeId, Host, Port, <<"pgsql">>),
   erlang:send_after(0, self(), reconnect),
   {ok, all, State}.

process(_In, _P = #data_point{}, State = #state{}) ->
   {ok, State};
process(_In, _B = #data_batch{}, State = #state{}) ->
   {ok, State}.


handle_info(query,
    State = #state{timer = Timer, client = C, period = Period, fn_id = FnId, resp_def = RespDef}) ->
   QueryMark = Timer#faxe_timer.last_time,
%%   lager:notice("query: ~p with ~p from: ~p, to :~p",
%%      [Q, [QueryMark-Period, QueryMark], faxe_time:to_iso8601(QueryMark-Period), faxe_time:to_iso8601(QueryMark)]),
   NewTimer = faxe_time:timer_next(Timer),
   %% do query
   {TsMy, Resp} = timer:tc(epgsql, prepared_query, [C, ?STMT, [QueryMark-Period, QueryMark]]),
   %%   lager:info("reading time: ~pms", [round(TsMy/1000)]),
   node_metrics:metric(?METRIC_READING_TIME, round(TsMy/1000), FnId),
%%   Resp = epgsql:prepared_query(C, ?STMT, [QueryMark-Period, QueryMark]),
%%   lager:debug("Resp: ~p",[Resp]),
%%   handle_response(Resp, RType, State#state{timer = NewTimer});
   NewState = State#state{timer = NewTimer},
   Result = faxe_epgsql_response:handle(Resp, RespDef),
%%   lager:notice("result: ~p",[Result]),
   case Result of
      ok ->
         {ok, NewState};
      {ok, Data} ->
         node_metrics:metric(?METRIC_ITEMS_IN, 1, FnId),
         {emit, {1, Data}, NewState};
      {error, Error} ->
         lager:warning("Error response from Crate: ", [Error]),
         {ok, NewState}
   end;


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
   connection_registry:connecting(),
%%   lager:info("db opts: ~p",[Opts]),
   case epgsql:connect(Opts) of
      {ok, C} ->
         connection_registry:connected(),
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
build_query(<<_Select:6/binary, Query/binary>>, TimeGroup, TimeField, GroupBys) ->
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