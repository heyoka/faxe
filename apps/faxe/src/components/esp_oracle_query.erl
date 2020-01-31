%% Date: 30.12.16 - 23:01
%% Query Oracle, time series data
%% Ⓒ 2019 heyoka
%%
-module(esp_oracle_query).
-author("Alexander Minichmair").

-include("faxe.hrl").

-behavior(df_component).
%% API
-export([init/3, process/3, options/0, handle_info/2, to_flowdata/2, handle_result/3]).

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

%%check_options() ->
%%   [{not_empty, [file]}].
%% jamdb_oracle:sql_query(Pid, "select connection, sent, received from tr_keepalive").

init(_NodeId, _Inputs, #{host := Host0, port := Port, user := User0, every := Every,
      pass := Pass0, service_name := DB, query := Q0, align := Align, result_type := ResType}) ->

   process_flag(trap_exit, true),
   Host = binary_to_list(Host0),
   User = binary_to_list(User0),
   Pass = binary_to_list(Pass0),
   ServiceName = binary_to_list(DB),
   Query = binary_to_list(clean_query(Q0)),

   DBOpts = [{host, Host}, {port, Port}, {user, User}, {password, Pass}, {service_name, ServiceName}],

   lager:warning("the QUERY before: ~p",[Q0]),
%%   Q = clean_query(Q0),
%%   Query = build_query(Q, TimeGroup, TimeField, GroupBys),
%%   lager:warning("the QUERY: ~p",[Query]),

   State = #state{host = Host, port = Port, user = User, pass = Pass, service_name = ServiceName, query = Query,
      db_opts = DBOpts, every = Every, align = Align, result_type = ResType},
   NewState = connect(State),
   {ok, all, NewState}.


clean_query(QueryBin) when is_binary(QueryBin) ->
   Q0 = re:replace(QueryBin, "\n|\t|\r|;", " ",[global, {return, binary}]),
   re:replace(Q0, "(\s){2,}", " ", [global, {return, binary}]).

process(_In, _P = #data_point{}, State = #state{}) ->
   {ok, State};
process(_In, _B = #data_batch{}, State = #state{}) ->
   {ok, State}.


handle_info(query, State = #state{timer = Timer, client = C, result_type = RType}) ->
%%   lager:info("do query oracle!!"),
%%   QueryMark = Timer#faxe_timer.last_time,
%%   lager:notice("query: ~p with ~p", [Q, [QueryMark-Period, QueryMark]]),
   NewTimer = faxe_time:timer_next(Timer),
   %% do query
   Res = jamdb_oracle:sql_query(C, State#state.query),
   {ok, [{result_set, Columns, [], Rows}]} = Res,
%%   lager:info("RESULT: ~nColumns: ~p~nRows: ~p",[Columns, Rows]),
   lager:info("RESULT: ~n ~p",[Res]),
   {_T, Data} = timer:tc(?MODULE, handle_result, [Columns, Rows, RType]),
%%   lager:notice("Data in ~p my: ~n~p",[T,Data]),
   dataflow:emit(Data),
   {ok, State#state{timer = NewTimer}};

handle_info({'EXIT', _C, _Reason}, State = #state{timer = Timer}) ->
   lager:notice("EXIT jamdb"),
   NewTimer = cancel_timer(Timer),
   NewState = connect(State),
   {ok, NewState#state{timer = NewTimer}};
handle_info(What, State) ->
   lager:warning("++other info : ~p",[What]),
   {ok, State}.

connect(State = #state{db_opts = Opts}) ->
   lager:warning("db opts: ~p",[Opts]),
   {ok, C} = jamdb_oracle:start_link(Opts),
   NewState = init_timer(State#state{client = C}),
   NewState.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

handle_result(Columns, Rows, <<"batch">>) ->
   to_flowdata(Columns, Rows);
handle_result(Columns, Rows, <<"point">>) ->
   to_flowdata_list(Columns, Rows).

%% result handling , output one data_point with all result rows (array)
to_flowdata_list(Columns, Rows) ->
   Batch = to_flowdata(Columns, Rows),
   FieldsList = [Fields || #data_point{fields = Fields} <- Batch#data_batch.points],
   #data_point{ts = faxe_time:now(), fields = #{<<"data">> => FieldsList}}.

%% result handling row_to_point %% one data_point per row in the result-set

to_flowdata(Columns, Rows) ->
   to_flowdata(Columns, lists:reverse(Rows), #data_batch{}, faxe_time:now()).

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