%% Date: 30.12.16 - 23:01
%% Query CrateDB, time series data
%% Ⓒ 2019 heyoka
%%
-module(esp_crate_query_cont).
-author("Alexander Minichmair").

-include("faxe.hrl").
-include("faxe_epgsql_response.hrl").

-behavior(df_component).
%% API
-export([init/3, process/3, options/0, handle_info/2, check_options/0, shutdown/1, metrics/0]).

-record(state, {
   host :: string(),
   port :: pos_integer(),
   query :: iodata(),
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
   setup_start :: true|false, %% whether we have get our starttime from an sql query
   stop :: undefined | pos_integer(), %% time (in data) at which to end queries
   stop_flow :: false | true|false, %% whether to stop the flow, when stop time is reached
   min_interval :: pos_integer(),
   interval :: pos_integer(), %% query interval that is in place
   offset :: non_neg_integer(),
   query_mark :: pos_integer(), %% this is the to mark, period will be substracted from it to get the from mark
   timer :: faxe_timer(),
   fn_id,
   debug_mode = false,
   response_def :: faxe_epgsql_response()
}).

-define(DB_OPTIONS, #{
   codecs => [
      {faxe_epgsql_codec, nil},
      {epgsql_codec_json, {jiffy, [], [return_maps]}}],
   timeout => 4000
}).

-define(TIMEFILTER_KEY, <<"$__timefilter">>).
%%-define(TIMEOUT_STATEMENT, <<"SET statement_timeout = ">>).
-define(KEEPALIVE_QUERY, <<"SELECT 1">>).
-define(KEEPALIVE_INTERVAL, 40000).
-define(START_QUERY_RETRY_INTERVAL, 4000).

-define(STMT, "stmt").


options() ->
   [
      {host, string, {crate, host}},
      {port, integer, {crate, port}},
      {ssl, boolean, false},
      {user, string, {crate, user}},
      {pass, string, {crate, user}},
      {database, string, {crate, database}},
      {query, any},
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
            (SF) when is_function(SF) -> true;
            (S)->  faxe_util:check_select_statement(S)
         end,
         <<" seems not to be a valid sql select statement">>},
      %% check if timefilter key is used in query
      {func, query,
         fun
            (Select) when is_binary(Select) ->
               check_timefilter(Select);
            (SF) when is_function(SF) -> true
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
   result_time_field := ResTimeField0, result_type := RType, filter_time_field := FilterTime, stop_flow := StopFlow}) ->

   process_flag(trap_exit, true),
   Host = binary_to_list(Host0),
   DBOpts0 = #{host => Host, port => Port, username => binary_to_list(User), ssl => Ssl,
      password => binary_to_list(Pass), database => DB},
   DBOpts = maps:merge(?DB_OPTIONS, DBOpts0),

   ResTimeField = case ResTimeField0 of undefined -> FilterTime; _ -> ResTimeField0 end,
   Response = faxe_epgsql_response:new(ResTimeField, erlang:binary_to_existing_atom(RType), <<"data">>),

   connection_registry:reg(NodeId, Host, Port, <<"pgsql">>),

   StartDelay = case Delay of undefined -> 0; _ -> faxe_time:duration_to_ms(Delay) end,
   %% init after maybe startdelay
   erlang:send_after(StartDelay, self(), {init2, Opts}),
   NewState = #state{
      host = Host, port = Port, user = User, pass = Pass, database = DB,
      db_opts = DBOpts, response_def = Response, fn_id = NodeId, stop_flow = StopFlow},
   {ok, all, NewState}.


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
         lager:warning("will not query into future ... send at: ~p for query-mark: ~p",
            [faxe_time:to_iso8601(QueryMark+Offset), faxe_time:to_iso8601(QueryMark)]),
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

handle_info({'EXIT', _C, Reason}, State = #state{}) ->
   lager:warning("EXIT epgsql with reason: ~p",[Reason]),
   State0 = cancel_timer(State),
   NewState = connect(State0),
   {ok, NewState};
handle_info(reconnect, State) ->
   {ok, connect(State)};
handle_info(start_setup, State) ->
   {ok, start_setup(State)};
handle_info(_What, State) ->
   {ok, State}.

shutdown(#state{client = C, stmt = _Stmt} = S) ->
   cancel_timer(S),
   catch epgsql:close(C).

connect(State = #state{db_opts = Opts, query = Q}) ->
   connection_registry:connecting(),
%%   lager:info("db opts: ~p",[Opts]),
   case epgsql:connect(Opts) of
      {ok, C} ->
         connection_registry:connected(),
         case epgsql:parse(C, ?STMT, Q, [int8, int8]) of
            {ok, Statement} ->
               erlang:send_after(0, self(), start_setup),
               State#state{client = C, stmt = Statement};
               %% setup start-time with a query:
%%               lager:notice("start_setup:~p",[lager:pr(NewState0, ?MODULE)]),
            Other ->
               lager:error("Can not parse prepared statement: ~p",[Other]),
               %error("parsing prepared statement failed!"),
               State
         end;
      {error, What} ->
         lager:warning("Error connecting to crate: ~p",[What]),
         State
   end.


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


setup_query(#{query := Q0, filter_time_field := _FilterTimeField}=QM, S=#state{}) when is_function(Q0) ->
   Q1 = faxe_lambda:execute(#data_point{}, Q0),
   case check_timefilter(Q1) of
      false -> error("Timefilter not found in statement !");
      true -> ok
   end,
   setup_query(QM#{query => Q1}, S);
setup_query(#{query := Q0, filter_time_field := FilterTimeField}, S=#state{}) ->
   Q = faxe_util:clean_query(Q0),
   Query = build_query(Q, FilterTimeField),
%%   lager:notice("QUERY: ~p",[Query]),
   S#state{query = Query}.

setup_query_start(S=#state{start = Start}) ->
   case faxe_util:check_select_statement(Start) of
      true ->
         S#state{setup_start = true};
      false ->
         prepare_start(S#state{setup_start = false})

   end.

start_setup(S=#state{setup_start = false}) ->
   start(S);
start_setup(S=#state{start = Start, client = Client}) ->

   case catch epgsql:equery(Client, Start) of
      {ok,[_TsCol],[{TimeStampString}]} ->
         NewState = prepare_start(S#state{start = TimeStampString, setup_start = false}),
         start(NewState);
      W ->
         lager:info("did not get starttime with query: ~p | ~p",[Start, W]),
         erlang:send_after(?START_QUERY_RETRY_INTERVAL, self(), start_setup),
         S
   end
.

start(S = #state{}) ->
   TRef = next_query(S),
   S#state{timer = TRef}.

%% stop
do_query(State = #state{query_mark = QueryMark, stop = Stop}) when Stop /= undefined andalso QueryMark > Stop ->
   lager:notice("stop is reached: ~p > ~p",[faxe_time:to_iso8601(QueryMark), faxe_time:to_iso8601(Stop)]),
   case State#state.stop_flow of
      true -> {stop, normal, State};
      false -> {ok, State}
   end;
do_query(State = #state{client = C, period = Period, query_mark = QueryMark, response_def = RespDef, fn_id = FnId}) ->
   %% do query
   FromTs = QueryMark-Period,
   {TsMy, Resp} = timer:tc(epgsql, prepared_query, [C, ?STMT, [FromTs, QueryMark]]),
   %%   lager:info("reading time: ~pms", [round(TsMy/1000)]),
   node_metrics:metric(?METRIC_READING_TIME, round(TsMy/1000), FnId),
   NewQueryMark = QueryMark+Period,
   NewState0 = State#state{query_mark = NewQueryMark},
   NewTimer = next_query(NewState0),
   NewState = NewState0#state{timer = NewTimer},
%%   lager:notice("from: ~p, to :~p (~p sec)",
%%      [faxe_time:to_iso8601(QueryMark-Period), faxe_time:to_iso8601(QueryMark), round(Period/1000)]),
   Result = faxe_epgsql_response:handle(Resp, RespDef#faxe_epgsql_response{default_timestamp = FromTs}),
%%   lager:notice("result: ~p",[Result]),
   case Result of
      ok ->
         {ok, NewState};
      {ok, Data} ->
         node_metrics:metric(?METRIC_ITEMS_IN, 1, FnId),
         {emit, {1, Data#data_batch{start = FromTs}}, NewState};
      {error, Error} ->
         lager:warning("Error response from Crate: ~p", [Error]),
         {ok, NewState}
   end.

next_query(#state{min_interval = Min, interval = Min}) ->
   erlang:send_after(Min, self(), query);
next_query(#state{query_mark = NewQueryMark, offset = Offset}) ->
%%   lager:info("send at: ~p for new query-mark: ~p",
%%      [faxe_time:to_iso8601(NewQueryMark+Offset), faxe_time:to_iso8601(NewQueryMark)]),
   faxe_time:send_at(NewQueryMark+Offset, query).


build_query(<<_Sel:6/binary, _Query/binary>> = Select, TimeField) ->
   TimeRangeClause = time_range(TimeField),
   binary:replace(Select, ?TIMEFILTER_KEY, TimeRangeClause).

time_range(TimeField) ->
   << TimeField/binary, " >= $1 AND ", TimeField/binary, " < $2" >>
.

cancel_timer(State = #state{timer = TRef}) ->
   catch erlang:cancel_timer(TRef),
   State#state{timer = undefined}.