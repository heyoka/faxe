%%
%% Query CrateDB continously for time series data
%% Queries are based on timestamps
%% Ⓒ 2021 heyoka
%%
-module(esp_crate_query_cont).
-author("Alexander Minichmair").

-include("faxe.hrl").
-include("faxe_epgsql_response.hrl").

-behavior(df_component).
%% API
-export([init/3, process/3, options/0, handle_info/2, check_options/0, shutdown/1, metrics/0, init/4]).

-record(state, {
   host :: string(),
   port :: pos_integer(),
   query :: iodata(),
   setup_query :: iodata(),
   setup_vars :: list(),
   setup_ts :: integer(),
   user :: string(),
   pass :: string(),
   database :: iodata(),
   client,
   client_ref,
   stmt, %% prepared pgsql statement
   db_opts, %% database options
   query_timeout :: non_neg_integer(),
   period, %% length of the time-range to use in the query
   start :: pos_integer(), %% time (in data) at which to start queries
   setup_start :: true|false, %% whether we have got our starttime from an sql query
   stop :: undefined | pos_integer(), %% time (in data) at which to end queries
   stop_flow :: false | true|false, %% whether to stop the flow, when stop time is reached
   min_interval :: pos_integer(),
   interval :: pos_integer(), %% query interval that is in place
   offset :: non_neg_integer(),
   query_mark :: pos_integer(), %% this is the 'to' mark, 'period' will be substracted from it to get the 'from' mark
   timer :: faxe_timer(),
   fn_id,
   debug_mode = false,
   response_def :: faxe_epgsql_response(),
   setup_done = false :: true|false, %% whether setup is done already

   %% done flags
%%   setup_time_query_done = false :: true|false,
   setup_start_done = false :: true|false
}).

-define(DB_OPTIONS, #{
   codecs => [
      {faxe_epgsql_codec, nil},
      {epgsql_codec_json, {jiffy, [], [return_maps]}}],
   timeout => 5000,
   tcp_opts => [{keepalive, true}]
}).

-define(TIMEFILTER_KEY, <<"$__timefilter">>).
-define(SETUP_QUERY_VAR_PLACEHOLDER, <<"$__var">>).
-define(SETUP_QUERY_START_PLACEHOLDER, <<"$__start">>).
%%-define(TIMEOUT_STATEMENT, <<"SET statement_timeout = ">>).
-define(KEEPALIVE_QUERY, <<"SELECT 1">>).
-define(KEEPALIVE_INTERVAL, 30000).
-define(START_QUERY_RETRY_INTERVAL, 4000).

-define(STMT, "stmt").


options() ->
   [
      {host, string, {crate, host}},
      {port, integer, {crate, port}},
      {ssl, boolean, {crate, tls, enable}},
      {user, string, {crate, user}},
      {pass, string, {crate, pass}},
      {database, string, {crate, database}},
      {query, any},
      {setup_query, any, undefined},
      {setup_vars, string_list, []},
      {setup_ts, any, undefined},
      {filter_time_field, string, <<"ts">>},
      {result_time_field, string, undefined},
      {offset, duration, <<"20s">>},
      {period, duration, <<"1h">>}, %% defaults to every
      {min_interval, duration, <<"5s">>}, %% should default to query_timeout (once implemented)
      {query_timeout, duration, <<"15s">>},
      {start, string},
      {start_delay, duration, undefined},
      {stop, string, undefined},
      {stop_flow, boolean, true},
      {result_type, string, <<"batch">>}
   ].

check_options() ->
   [
      {one_of, result_type, [<<"batch">>, <<"point">>]},
      %% check for valid select statement
      {func, query,
         fun
            (SF) when is_record(SF, faxe_lambda) -> true;
            (S)->  faxe_util:check_select_statement(S)
         end,
         <<" seems not to be a valid sql select statement">>},
      %% check setup_query for valid select statement
      {func, setup_query,
         fun
            (undefined) -> true;
            (SF) when is_record(SF, faxe_lambda) -> true;
            (S)->  faxe_util:check_select_statement(S)
         end,
         <<" seems not to be a valid sql select statement">>},
      %% check if setup_ts option is given as a valid iso8601 datetime string or an integer
      {func, setup_ts,
         fun
            (undefined) -> true;
            (Value) when is_binary(Value) ->
               case catch(time_format:iso8601_to_ms(Value)) of
                  Ts when is_integer(Ts) ->
                     true;
                  _ ->
                     false
               end;
            (ValueInt) when is_integer(ValueInt) ->
               true;
            (_) ->
               false
         end,
         <<" seems not to be a ISO8601 datetime string or an integer">>
      },
      %% check if timefilter key is used in query
      {func, query,
         fun
            (Select) when is_binary(Select) ->
               check_timefilter(Select);
            (SF) when is_record(SF, faxe_lambda) -> true
         end,
         <<" timefilter key '", ?TIMEFILTER_KEY/binary, "' missing in query">>
      },
      %% check if start option is given as a valid iso8601 datetime string
      {func, start,
         fun(Value) ->
            case catch(time_format:iso8601_to_ms(Value)) of
               Ts when is_integer(Ts) ->
                  true;
               _ ->
                  faxe_util:check_select_statement(Value)
            end
         end,
         <<" seems not to be a ISO8601 datetime string">>
      },
      %% check if stop option is given as a valid iso8601 datetime string or undefined
      {func, stop,
         fun
            (undefined) -> true;
            (Value) ->
                  case catch(time_format:iso8601_to_ms(Value)) of
                       Ts when is_integer(Ts) -> true;
                       _ -> false
                  end
         end,
         <<" seems not to be a ISO8601 datetime string">>
      }
   ].

check_timefilter(Query) ->
   binary:match(Query, ?TIMEFILTER_KEY) /= nomatch.

metrics() ->
   [
      {?METRIC_BYTES_READ, meter, []},
      {?METRIC_READING_TIME, histogram, [slide, 30], "The number of milliseconds it took the query-result to return."}
   ].

init(NodeId, _Inputs, Opts = #{
   host := Host0, port := Port, user := User, pass := Pass, ssl := Ssl, database := DB, start_delay := Delay,
   setup_query := SetupQuery, setup_vars := SetupVars, setup_ts := SetupTs0,
   result_time_field := ResTimeField0, result_type := RType, filter_time_field := FilterTime, stop_flow := StopFlow}) ->

   process_flag(trap_exit, true),
   Host = binary_to_list(Host0),
   DBOpts0 = #{host => Host, port => Port, username => faxe_util:to_list(User),
      ssl => Ssl, password => faxe_util:to_list(Pass), database => DB, async => self()},
   DBOpts = maps:merge(?DB_OPTIONS, DBOpts0),

   ResTimeField = case ResTimeField0 of undefined -> FilterTime; _ -> ResTimeField0 end,
   Response = faxe_epgsql_response:new(ResTimeField, erlang:binary_to_existing_atom(RType), <<"data">>),

   connection_registry:reg(NodeId, Host, Port, <<"pgsql">>),

   StartDelay = case Delay of undefined -> 0; _ -> faxe_time:duration_to_ms(Delay) end,
   SetupTs = case SetupTs0 of undefined -> undefined; STs when is_binary(STs) -> time_format:iso8601_to_ms(STs); _ -> SetupTs0 end,
   %% init after maybe startdelay
   erlang:send_after(StartDelay, self(), {init2, Opts}),
   lager:info("~p init",[?MODULE]),
   NewState = #state{
      host = Host, port = Port, user = User, pass = Pass, database = DB,
      setup_query = SetupQuery, setup_vars = SetupVars, setup_ts = SetupTs,
      db_opts = DBOpts, response_def = Response, fn_id = NodeId, stop_flow = StopFlow},
   {ok, true, NewState}.


init(NodeId, _Inputs, #{port := Port, host := Host0}, #node_state{state = State}) ->
   lager:warning("init with persisted state: ~p", [State]),
   process_flag(trap_exit, true),
   Host = binary_to_list(Host0),
   connection_registry:reg(NodeId, Host, Port, <<"pgsql">>),
   %% connect
   erlang:send_after(0, self(), reconnect),
   {ok, true, State}.

process(_In, _P = #data_point{}, State = #state{}) ->
   {ok, State};
process(_In, _B = #data_batch{}, State = #state{}) ->
   {ok, State}.

handle_info(query, State = #state{timer = _Timer, query_mark = QueryMark, offset = Offset}) ->
   Now = faxe_time:now(),
%%   lager:notice("QUERY !! Now: ~p, QueryMark: ~p diff:~p",
%%      [faxe_time:to_iso8601(Now), faxe_time:to_iso8601(QueryMark), QueryMark-Now]),
   %% we are ahead of time
   case QueryMark > Now of
      true ->
%%         lager:warning("will not query into future ... send at: ~p for query-mark: ~p",
%%            [faxe_time:to_iso8601(QueryMark+Offset), faxe_time:to_iso8601(QueryMark)]),
         faxe_time:send_at(QueryMark+Offset, query),
         {ok, State#state{interval = undefined}};
      false ->
         do_query(State)
   end;

handle_info({init2, StartOpts}, State = #state{}) ->
   %% prepare and convert time(r) related options
   State0 = setup_time(StartOpts, State),
   State1 = setup_query(StartOpts, State0),
   erlang:send_after(0, self(), reconnect),
   {ok, State1};

handle_info({'EXIT', C, normal}, State = #state{client = C}) ->
   lager:notice("epgsql client EXITED normal"),
   {ok, State};
handle_info({'EXIT', _C, Reason}, State = #state{}) ->
   case Reason of
      sock_closed -> lager:warning("EXIT epgsql with reason: sock_closed");
      Err -> lager:warning("EXIT epgsql with reason: ~p",[Err])
   end,
   State0 = cancel_timer(State),
   erlang:send_after(1000, self(), reconnect),
   {ok, State0};
handle_info(reconnect, State) ->
   {ok, connect(State)};
handle_info(start_setup, State) ->
   {ok, start_setup(State)};
handle_info(_What, State) ->
   {ok, State}.

shutdown(#state{client = C, stmt = _Stmt} = S) ->
   connection_registry:disconnected(),
   cancel_timer(S),
   epgsql:cancel(C),
%%   lager:notice("shutdown"),
   catch epgsql:close(C).

connect(State = #state{db_opts = Opts, query = Q}) ->
   connection_registry:connecting(),
%%   lager:info("CONNECT ~p db opts: ~p",[?MODULE, Opts]),
   case epgsql:connect(Opts) of
      {ok, C} ->
         connection_registry:connected(),
         case epgsql:parse(C, ?STMT, Q, [int8, int8]) of
            {ok, Statement} ->
               after_connect(State#state{client = C, stmt = Statement});
            Other ->
               lager:error("Can not parse prepared statement: ~p",[Other]),
               State
         end;

      {error, What} ->
         lager:warning("Error connecting to crate: ~p",[What]),
         State
   end.

after_connect(State = #state{setup_done = false}) ->
   erlang:send_after(50, self(), start_setup),
   State;
after_connect(State = #state{}) ->
   start(State).



%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
setup_time(#{
   query_timeout := QueryTimeout0, offset := Offset0, period := Period0,
   start := Start0, stop := Stop0, min_interval := MinInterval0
}, State) ->
   %% prepare and convert time(r) related options
   QueryTimeout = faxe_time:duration_to_ms(QueryTimeout0),
   Offset = faxe_time:duration_to_ms(Offset0),
   MinInterval = faxe_time:duration_to_ms(MinInterval0),

   NewState = setup_query_start(State#state{start = Start0, stop = Stop0, period = Period0}),

   NewState#state{
      query_timeout = QueryTimeout, offset = Offset, min_interval = MinInterval, interval = MinInterval}.


prepare_start(State = #state{start = Start0, period = Period0, stop = Stop0}) ->
   %% period also used for start alignment
   Period = faxe_time:duration_to_ms(Period0),
   PeriodDuration = faxe_time:binary_to_duration(Period0),
   Start1 = time_format:iso8601_to_ms(Start0),
   Start = faxe_time:align(Start1, PeriodDuration),

   Stop =
      case Stop0 of
         undefined -> Stop0;
         _ ->
            Stop1 = time_format:iso8601_to_ms(Stop0),
            faxe_time:align(Stop1, PeriodDuration)
      end,

   QueryMark = Start + Period,
   State#state{start = Start, stop = Stop, period = Period, query_mark = QueryMark}.

%% setup the main query (the continous one)
setup_query(#{query := Q0, filter_time_field := _FilterTimeField}=QM, S=#state{}) when is_record(Q0, faxe_lambda) ->
   Q1 = faxe_lambda:execute(#data_point{}, Q0),
   case check_timefilter(Q1) of
      false -> error("Timefilter not found in statement !");
      true -> ok
   end,
   setup_query(QM#{query => Q1}, S);
setup_query(#{query := Q0, filter_time_field := FilterTimeField}, S=#state{}) ->
   Q = faxe_util:clean_query(Q0),
   Query = build_query(Q, FilterTimeField),
%%   lager:notice("QUERY: ~s",[Query]),
   S#state{query = Query}.

setup_query_start(S=#state{start = Start}) ->
   case faxe_util:check_select_statement(Start) of
      true ->
         S#state{setup_start = true};
      false ->
         prepare_start(S#state{setup_start = false})

   end.

start_setup(S=#state{setup_start = false}) ->
   %% do the setup query here
   maybe_query_setup(S);
start_setup(S=#state{start = Start, client = Client}) ->
   case catch epgsql:squery(Client, Start) of
      {ok,[_TsCol],[{TimeStampString}]} when is_binary(TimeStampString) ->
         NewState = prepare_start(S#state{start = TimeStampString, setup_start = false}),
         %% do the setup query here
         maybe_query_setup(NewState);
      W ->
         lager:info("did not get starttime with query: ~p | ~p",[Start, W]),
         erlang:send_after(?START_QUERY_RETRY_INTERVAL, self(), start_setup),
         S
   end
.


%% that is a very bad naming here: change this, as setup_query and query_setup can be easily mixed up
%% this is the setup_query (nothing to do with setting up the main query ;) )
maybe_query_setup(S = #state{setup_query = undefined}) ->
   start(S);
maybe_query_setup(S = #state{setup_query = SetupQuery, setup_vars = [], client = C,
      response_def = RespDef, setup_ts = Ts, start = StartTime}) ->

   Query = prepare_setup_query(SetupQuery, <<>>, StartTime),
   case do_setup_query(C, Query, RespDef#faxe_epgsql_response{default_timestamp = Ts}) of
      {ok, R, _} ->
%%         lager:notice("result from setup query: ~p",[R]),
         dataflow:emit(R#data_batch{start = Ts});
      _ -> ok
   end,
   start(S);
maybe_query_setup(S = #state{setup_query = SetupQuery, setup_vars = Vars, client = C,
      response_def = RespDef, setup_ts = Ts, start = StartTime}) ->
   lager:notice("setup query with ~p vars",[length(Vars)]),
   FoldFun =
   fun(Var, Acc = #data_batch{points = AccPoints}) ->
      Q = prepare_setup_query(SetupQuery, Var, StartTime),
      case do_setup_query(C, Q, RespDef#faxe_epgsql_response{default_timestamp = Ts}) of
         {ok, #data_batch{points = Points}, _} ->
            Acc#data_batch{points = AccPoints++Points};
         {error, _What} ->
            Acc
      end
   end,
   ResDataBatch = lists:foldl(FoldFun, #data_batch{start = Ts}, Vars),
%%   lager:notice("result from setup QUERY: ~p",[ResDataBatch]),
   dataflow:emit(ResDataBatch),
   start(S).

prepare_setup_query(SetupQuery, Var, StartTime) ->
   Q0 = binary:replace(SetupQuery, ?SETUP_QUERY_VAR_PLACEHOLDER, Var, [global]),
   binary:replace(Q0, ?SETUP_QUERY_START_PLACEHOLDER, integer_to_binary(StartTime), [global]).

do_setup_query(Client, Query, RespDef) ->
%%   lager:notice("setup query ~p",[Query]),
   lager:info("~p do_setup_query",[?MODULE]),
   case catch epgsql:equery(Client, Query) of
      {ok, _, _} = Res ->
          faxe_epgsql_response:handle(Res, RespDef);
      W ->
         lager:warning("setup-query failed: ~p | ~p",[Query, W]),
         {error, W}
   end.



start(S = #state{}) ->
   TRef = next_query(S),
   S#state{timer = TRef, setup_done = true}.

%% stop
do_query(State = #state{query_mark = QMark, stop = Stop, client = C}) when Stop /= undefined andalso QMark > Stop ->
   lager:notice("stop is reached: ~p > ~p",[faxe_time:to_iso8601(QMark), faxe_time:to_iso8601(Stop)]),
   epgsql:close(C),
   {ok, State};
do_query(State = #state{client = C, period = Period, query_mark = QueryMark, fn_id = FnId, stmt = Stmt}) ->
   %% do query
   FromTs = QueryMark-Period,
%%   lager:notice("from: ~p, to :~p (~p sec)",
%%      [faxe_time:to_iso8601(QueryMark-Period), faxe_time:to_iso8601(QueryMark), round(Period/1000)]),
   case catch timer:tc(epgsql, prepared_query, [C, Stmt, [FromTs, QueryMark]]) of
      {TsMy, Resp} when is_integer(TsMy) ->
         node_metrics:metric(?METRIC_READING_TIME, round(TsMy/1000), FnId),
         handle_result(Resp, FromTs, State);
      Err ->
         lager:error("Error reading from DB: ~p",[Err]),
         case is_process_alive(C) of
            true ->
               QTimer = next_query(State),
               {ok, State#state{timer = QTimer}};
            false ->
%%               erlang:send_after(500, self(), reconnect),
               {ok, State}
         end
   end.
%%   {TsMy, Resp} = timer:tc(epgsql, prepared_query, [C, ?STMT, [FromTs, QueryMark]]),
%%   lager:info("read from ~p to ~p",[FromTs, QueryMark]),
   %%   lager:info("reading time: ~pms", [round(TsMy/1000)]),
%%   node_metrics:metric(?METRIC_READING_TIME, round(TsMy/1000), FnId),


%%   NewQueryMark = QueryMark+Period,
%%   NewState0 = State#state{query_mark = NewQueryMark},
%%   NewTimer = next_query(NewState0),
%%   NewState = NewState0#state{timer = NewTimer},
%%%%   lager:notice("from: ~p, to :~p (~p sec)",
%%%%      [faxe_time:to_iso8601(QueryMark-Period), faxe_time:to_iso8601(QueryMark), round(Period/1000)]),
%%%%   Result = faxe_epgsql_response:handle(Resp, RespDef#faxe_epgsql_response{default_timestamp = FromTs}),
%%   case catch faxe_epgsql_response:handle(Resp, RespDef) of
%%      ok ->
%%         {ok, NewState};
%%      {ok, Data, NewResponseDef} ->
%%%%         lager:notice("JB: ~s",[flowdata:to_json(Data)]),
%%         node_metrics:metric(?METRIC_ITEMS_IN, 1, FnId),
%%         {emit, {1, Data#data_batch{start = FromTs}}, NewState#state{response_def = NewResponseDef}};
%%      {error, Error} ->
%%         lager:warning("Error response from Crate: ~p", [Error]),
%%         {ok, NewState};
%%      What ->
%%         lager:error("Error handling CRATE response: ~p",[What]),
%%         {stop, invalid, NewState}
%%   end.

handle_result(Resp, FromTs, State = #state{period = Period, query_mark = QueryMark, response_def = RespDef, fn_id = FnId}) ->
   NewQueryMark = QueryMark+Period,
   NewState0 = State#state{query_mark = NewQueryMark},
   NewTimer = next_query(NewState0),
   NewState = NewState0#state{timer = NewTimer},
   case catch faxe_epgsql_response:handle(Resp, RespDef) of
      ok ->
         {ok, NewState};
      {ok, Data, NewResponseDef} ->
%%         lager:notice("JB: ~s",[flowdata:to_json(Data)]),
         node_metrics:metric(?METRIC_ITEMS_IN, 1, FnId),
         {emit, {1, Data#data_batch{start = FromTs}}, NewState#state{response_def = NewResponseDef}};
      {error, Error} ->
         lager:warning("Error response from Crate: ~p", [Error]),
         {ok, NewState};
      What ->
         lager:error("Error handling CRATE response: ~p",[What]),
         {stop, invalid, NewState}
   end.


next_query(#state{min_interval = Min, interval = Min}) ->
   erlang:send_after(Min, self(), query);
next_query(#state{query_mark = NewQueryMark, offset = Offset}) ->
%%   lager:info("send at: ~p for new query-mark: ~p",
%%      [faxe_time:to_iso8601(NewQueryMark+Offset), faxe_time:to_iso8601(NewQueryMark)]),
   faxe_time:send_at(NewQueryMark+Offset, query).


build_query(<<_Sel:6/binary, _Query/binary>> = Select, TimeField) ->
   TimeRangeClause = time_range(TimeField),
   binary:replace(Select, ?TIMEFILTER_KEY, TimeRangeClause, [global]).

time_range(TimeField) ->
   << TimeField/binary, " >= $1 AND ", TimeField/binary, " < $2" >>
.

cancel_timer(State = #state{timer = TRef}) ->
   catch erlang:cancel_timer(TRef),
   State#state{timer = undefined}.