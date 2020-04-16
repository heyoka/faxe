%% Date: 30.12.16 - 23:01
%% Query Oracle, time series data
%% Ⓒ 2019 heyoka
%%
-module(esp_oracle_query).
-author("Alexander Minichmair").

-include("faxe.hrl").

-behavior(df_component).
%% API
-export([init/3, process/3, options/0, handle_info/2, to_flowdata/3, handle_result/4, check_options/0]).

-record(state, {
   host :: string(),
   port :: non_neg_integer(),
   query :: iodata(),
   result_type :: binary(),
   user :: string(), %% Schema
   pass :: string(), %%
   service_name :: string(),
%%   database :: iodata(),
   client,
   client_ref,
   stmt,
   db_opts,
   every,
   align = false,
   timer
}).

-define(DB_OPTIONS, #{
   timeout => 3000
}).

-define(STMT, "stmt").


options() ->
   [
      {host, string},
      {port, integer},
      {user, string},
      {pass, string, <<>>},
      {service_name, string},
      {query, string},
      {result_type, string, <<"batch">>}, %% 'batch' or 'point'
      {time_field, string, <<"ts">>},
      {every, duration, <<"5s">>},
      {align, is_set},
      {limit, string, <<"30">>}].

check_options() ->
   [
      {func, query, fun faxe_util:check_select_statement/1,
         <<"seems not to be a valid sql select statement">>}
   ].

init(_NodeId, _Inputs, #{host := Host0, port := Port, user := User0, every := Every,
      pass := Pass0, service_name := DB, query := Q0, align := Align, result_type := ResType}) ->

   process_flag(trap_exit, true),
   Host = binary_to_list(Host0),
   User = binary_to_list(User0),
   Pass = binary_to_list(Pass0),
   ServiceName = binary_to_list(DB),
   Query = binary_to_list(faxe_util:clean_query(Q0)),

   DBOpts = [{host, Host}, {port, Port}, {user, User}, {password, Pass}, {service_name, ServiceName}],

   lager:notice("the QUERY : ~p",[Query]),

   State = #state{host = Host, port = Port, user = User, pass = Pass, service_name = ServiceName, query = Query,
      db_opts = DBOpts, every = Every, align = Align, result_type = ResType},
   erlang:send_after(0, self(), reconnect),
   {ok, all, State}.


process(_In, _P = #data_point{}, State = #state{}) ->
   {ok, State};
process(_In, _B = #data_batch{}, State = #state{}) ->
   {ok, State}.

handle_info(reconnect, State) ->
   {ok, connect(State)};
handle_info(query, State = #state{timer = Timer, client = C, result_type = RType}) ->
%% use timestamp from timer, in case of aligned queries, we have a straight
   Timestamp = Timer#faxe_timer.last_time,
   NewTimer = faxe_time:timer_next(Timer),
   %% do query
   Res = jamdb_oracle:sql_query(C, State#state.query),
   handle_response(Res, Timestamp, RType),
   {ok, State#state{timer = NewTimer}};

handle_info({'EXIT', _C, _Reason}, State = #state{timer = Timer}) ->
   lager:notice("EXIT jamdb"),
   NewTimer = cancel_timer(Timer),
   erlang:send_after(1000, self(), reconnect),
   {ok, State#state{timer = NewTimer}};
handle_info(What, State) ->
   lager:warning("++other info : ~p",[What]),
   {ok, State}.

-spec connect(#state{}) -> #state{}.
connect(State = #state{db_opts = Opts}) ->
   case jamdb_oracle:start_link(Opts) of
      {ok, C} ->
         init_timer(State#state{client = C});
      {error, _What} ->
         State
   end.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
handle_response({ok, [{result_set, Columns, [], Rows}]}, Timestamp, RType) ->
   lager:info("RESULT-length: ~n ~p",[length(Rows)]),
   Data = handle_result(Columns, Rows, Timestamp, RType),
   dataflow:emit(Data);
handle_response({ok,[{proc_result,_ ,Message}]}, _, _) ->
   lager:warning("No query-result, but message: ~p",[Message]);
handle_response({error,socket,closed}, _, _) ->
   lager:warning("Got closed socket when reading from db");
handle_response({error, Type, Reason}, _, _) ->
   lager:warning("Got error when reading from db: ~p: ~p",[Type, Reason]);
handle_response(What, _, _) ->
   lager:warning("Unexpected query-response: ~p", [What]).


handle_result(Columns, Rows, Ts, <<"batch">>) ->
   to_flowdata(Columns, Rows, Ts);
handle_result(Columns, Rows, Ts, <<"point">>) ->
   to_flowdata_list(Columns, Rows, Ts).

%% result handling , output one data_point with all result rows (array)
to_flowdata_list(Columns, Rows, Ts) ->
   Batch = to_flowdata(Columns, Rows, Ts),
   FieldsList = [Fields || #data_point{fields = Fields} <- Batch#data_batch.points],
   #data_point{ts = Ts, fields = #{<<"data">> => FieldsList}}.

%% result handling row_to_point %% one data_point per row in the result-set

to_flowdata(Columns, Rows, Ts) ->
   to_flowdata(Columns, lists:reverse(Rows), #data_batch{}, Ts).

to_flowdata(_C, [], Batch=#data_batch{}, _) ->
   Batch;
to_flowdata(Columns, [ValRow|Values], Batch=#data_batch{points = Points}, Ts) ->
   Point = row_to_datapoint(Columns, ValRow, #data_point{ts = Ts}),
   to_flowdata(Columns, Values, Batch#data_batch{points = [Point|Points]}, Ts).

row_to_datapoint([], [], Point) ->
   Point;
row_to_datapoint([C|Columns], [Val|Row], Point) ->
   P = flowdata:set_field(Point, C, decode(Val)),
   row_to_datapoint(Columns, Row, P).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


%% {{2020,1,22},{8,3,6.827},"+01:00"} to iso8601
decode({{_Y,_M_, _D}=Date, {_H, _M, _SecFrac} = Time, TZOffset}) ->
   Iso = faxe_time:to_iso8601({Date, Time}),
   binary:replace(Iso, <<"Z">>, list_to_binary(TZOffset));
decode(String) when is_list(String) ->
   list_to_binary(String);
decode({Number}) when is_number(Number) ->
   Number;
decode(Other) ->
   Other.


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
-endif.