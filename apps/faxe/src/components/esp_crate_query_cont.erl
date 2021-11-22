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
   stop :: undefined | pos_integer(), %% time (in data) at which to end queries
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
      {epgsql_codec_json, {jiffy, [], [return_maps]}}],
   timeout => 4000
}).

-define(TIMEFILTER_KEY, <<"$__timefilter">>).
%%-define(TIMEOUT_STATEMENT, <<"SET statement_timeout = ">>).
-define(KEEPALIVE_QUERY, <<"SELECT 1">>).
-define(KEEPALIVE_INTERVAL, 40000).

-define(STMT, "stmt").


options() ->
   [
      {host, string, {crate, host}},
      {port, integer, {crate, port}},
      {ssl, boolean, false},
      {user, string, {crate, user}},
      {pass, string, {crate, user}},
      {database, string, {crate, database}},
      {query, string},
      {filter_time_field, string, <<"ts">>},
      {result_time_field, string, undefined},
      {offset, duration, <<"17s">>},
      {period, duration, <<"1h">>}, %% defaults to every
      {min_interval, duration, <<"5s">>}, %% should default to query_timeout (once implemented)
      {query_timeout, duration, <<"15s">>},
      {start, string},
      {stop, string, undefined},
      {result_type, string, <<"batch">>}
   ].

check_options() ->
   [
      {one_of, result_type, [<<"batch">>, <<"point">>]},
      %% check for valid select statement
      {func, query, fun faxe_util:check_select_statement/1, <<" seems not to be a valid sql select statement">>},
      %% check if timefilter key is used in query
      {func, query,
         fun(Select) ->
            case binary:match(Select, ?TIMEFILTER_KEY) of
               nomatch -> false;
               _ -> true
            end
         end,
         <<" timefilter key '", ?TIMEFILTER_KEY/binary, "' missing in query">>
      },
      %% check if start option is given as a valid iso8601 datetime string
      {func, start,
         fun(Value) ->
            case catch(time_format:iso8601_to_ms(Value)) of Ts when is_integer(Ts) -> true; _ -> false end
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

metrics() ->
   [
      {?METRIC_BYTES_READ, meter, []},
      {?METRIC_READING_TIME, histogram, [slide, 30], "The number of milliseconds it took the query-result to return."}
   ].

init(NodeId, _Inputs, #{
   host := Host0, port := Port, user := User, pass := Pass, ssl := Ssl,
   database := DB, query := Q0, filter_time_field := FilterTimeField, result_time_field := ResTimeField,
   result_type := RType, query_timeout := QueryTimeout0,
   offset := Offset0, period := Period0, start := Start0, stop := Stop0, min_interval := MinInterval0
   }) ->

   process_flag(trap_exit, true),
   Host = binary_to_list(Host0),
   Opts = #{host => Host, port => Port, username => binary_to_list(User), ssl => Ssl,
      password => binary_to_list(Pass), database => DB},
   DBOpts = maps:merge(?DB_OPTIONS, Opts),

   %% prepare and convert time(r) related options
   QueryTimeout = faxe_time:duration_to_ms(QueryTimeout0),
   Offset = faxe_time:duration_to_ms(Offset0),
   MinInterval = faxe_time:duration_to_ms(MinInterval0),
   Period = faxe_time:duration_to_ms(Period0),
   Start1 = time_format:iso8601_to_ms(Start0),
   Start = faxe_time:align(Start1, faxe_time:binary_to_duration(Period0)),
   Stop =
   case Stop0 of
      undefined -> Stop0;
      _ ->
         Stop1 = time_format:iso8601_to_ms(Stop0),
         faxe_time:align(Stop1, faxe_time:binary_to_duration(Period0))
   end,

   QueryMark = Start + Period,

   lager:notice("before: ~p",[Q0]),
   Q = faxe_util:clean_query(Q0),
   lager:notice("after: ~p",[Q]),
   Query = build_query(Q, FilterTimeField),
   lager:warning("the QUERY: ~s",[Query]),

   Response = faxe_epgsql_response:new(ResTimeField, erlang:binary_to_existing_atom(RType), <<"data">>),

   State = #state{
      host = Host, port = Port, user = User, pass = Pass, database = DB, query = Query,
      db_opts = DBOpts, query_timeout = QueryTimeout,
      offset = Offset, period = Period, start = Start, stop = Stop,
      query_mark = QueryMark, min_interval = MinInterval, interval = MinInterval,
      response_def = Response,
      fn_id = NodeId},

   lager:notice("state at init: ~p",[lager:pr(State, ?MODULE)]),
   connection_registry:reg(NodeId, Host, Port, <<"pgsql">>),
   erlang:send_after(0, self(), reconnect),
   {ok, all, State}.

process(_In, _P = #data_point{}, State = #state{}) ->
   {ok, State};
process(_In, _B = #data_batch{}, State = #state{}) ->
   {ok, State}.


handle_info(query, State = #state{timer = _Timer, query_mark = QueryMark, offset = Offset}) ->
   Now = faxe_time:now(),
   lager:notice("QUERY !! Now: ~p, QueryMark: ~p diff:~p",
      [faxe_time:to_iso8601(Now), faxe_time:to_iso8601(QueryMark), QueryMark-Now]),
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

handle_info({'EXIT', _C, Reason}, State = #state{}) ->
   lager:warning("EXIT epgsql with reason: ~p",[Reason]),
   State0 = cancel_timer(State),
   NewState = connect(State0),
   {ok, NewState};
handle_info(reconnect, State) ->
   {ok, connect(State)};
handle_info(What, State) ->
   lager:warning("++other info : ~p",[What]),
   {ok, State}.

shutdown(#state{client = C, stmt = _Stmt} = S) ->
   cancel_timer(S),
   catch epgsql:close(C).

connect(State = #state{db_opts = Opts, query = Q}) ->
   connection_registry:connecting(),
   lager:info("db opts: ~p",[Opts]),
   case epgsql:connect(Opts) of
      {ok, C} ->
         connection_registry:connected(),
         case epgsql:parse(C, ?STMT, Q, [int8, int8]) of
            {ok, Statement} ->
               NewState = State#state{client = C, stmt = Statement},
               TRef = next_query(NewState),
               NewState#state{timer = TRef};
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

do_query(State = #state{query_mark = QueryMark, stop = Stop}) when Stop /= undefined andalso QueryMark > Stop ->
   lager:warning("stop is reached: ~p > ~p",[QueryMark, Stop]),
%%   {stop, normal, State};
   {ok, State};
do_query(State = #state{client = C, period = Period, query_mark = QueryMark, response_def = RespDef, fn_id = FnId}) ->
   %% do query
   {TsMy, Resp} = timer:tc(epgsql, prepared_query, [C, ?STMT, [QueryMark-Period, QueryMark]]),
   %%   lager:info("reading time: ~pms", [round(TsMy/1000)]),
   node_metrics:metric(?METRIC_READING_TIME, round(TsMy/1000), FnId),
   NewQueryMark = QueryMark+Period,
   NewState0 = State#state{query_mark = NewQueryMark},
   NewTimer = next_query(NewState0),
   NewState = NewState0#state{timer = NewTimer},
   lager:notice("from: ~p, to :~p (~p sec)",
      [faxe_time:to_iso8601(QueryMark-Period), faxe_time:to_iso8601(QueryMark), round(Period/1000)]),
%%   lager:notice("new querymark: ~p",[faxe_time:to_iso8601(NewQueryMark)]),
   Result = faxe_epgsql_response:handle(Resp, RespDef#faxe_epgsql_response{default_timestamp = QueryMark-Period}),
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
   end.

next_query(#state{min_interval = Min, interval = Min}) ->
   erlang:send_after(Min, self(), query);
next_query(#state{query_mark = NewQueryMark, offset = Offset}) ->
   lager:notice("send at: ~p for new query-mark: ~p",
      [faxe_time:to_iso8601(NewQueryMark+Offset), faxe_time:to_iso8601(NewQueryMark)]),
   faxe_time:send_at(NewQueryMark+Offset, query).


build_query(<<_Sel:6/binary, _Query/binary>> = Select, TimeField) ->
   TimeRangeClause = time_range(TimeField),
%%   Select =
%%      <<
%%         "SELECT ",
%%         TimeField/binary,
%%         ", ", Query/binary
%%   >>,
   binary:replace(Select, ?TIMEFILTER_KEY, TimeRangeClause).

time_range(TimeField) ->
   << TimeField/binary, " >= $1 AND ", TimeField/binary, " < $2" >>
.

cancel_timer(State = #state{timer = TRef}) ->
   catch erlang:cancel_timer(TRef),
   State#state{timer = undefined}.