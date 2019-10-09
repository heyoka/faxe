%% Date: 30.12.16 - 23:01
%% Query PostgreSQL (or compatible ie: CrateDB) database
%% Ⓒ 2019 heyoka
%%
-module(esp_postgresql_query).
-author("Alexander Minichmair").

-include("faxe.hrl").

-behavior(df_component).
%% API
-export([init/3, process/3, options/0, handle_info/2]).

-record(state, {
   host :: string(),
   port :: non_neg_integer(),
   query :: iodata(),
   user :: string(),
   pass :: string(),
   database :: iodata(),
   client,
   client_ref,
   stmt
}).

options() ->
   [
      {host, string},
      {port, integer},
      {query, string, <<"">>},
      {user, string},
      {pass, string, <<>>},
      {database, string}].

%%check_options() ->
%%   [{not_empty, [file]}].

init(_NodeId, _Inputs, #{host := Host, port := Port, user := User,
      pass := Pass, database := DB, query := Q}) ->
   State = #state{host = binary_to_list(Host), port = Port, user = User,
      pass = Pass, database = DB, query = Q},
   NewState = connect(State),
   {ok, all, NewState}.

process(_In, _P = #data_point{}, State = #state{}) ->

   {ok, State};
process(_In, _B = #data_batch{}, State = #state{}) ->

   {ok, State}.

handle_info({_C, _Ref, connected},
    State=#state{client_ref = _Ref, client = C, query = Q}) ->
   %% connected
   lager:notice("epgsql connected!"),
   {C, _Ref1, {ok, Stmt}} = epgsqla:parse(C, "stmt", Q, []),
   lager:notice("stmt: ~p",[Stmt]),
   Result = epgsqla:equery(C, Stmt, []),
   lager:warning("Result from CrateDB for query: ~p ===> ~n~p",[Stmt, Result]),
   {ok, State#state{stmt = Stmt}};
handle_info({C, Ref, Error = {error, _}}, State) ->
   lager:error("~p Error: ~p", [?MODULE, Error]),
   {ok, State#state{client_ref = undefined, client = undefined}};
handle_info({'EXIT', _C, _Reason}, State) ->
   lager:notice("EXIT epgsql"),
   NewState = connect(State),
   {ok, NewState};
handle_info(What, State) ->
   lager:warning("++other info : ~p",[What]),
   {ok, State}.

connect(State = #state{host = Host, port = Port, user = User, pass = Pass, database = DB, query = Q}) ->
   {ok, C} = epgsql:connect(#{host => Host, port => Port, username => User, pass => Pass, database => DB}),
%%   Ref = epgsqla:connect(#{host => Host, port => Port, username => User, pass => Pass, database => DB}),
   {ok, Columns, Rows} = epgsql:equery(C, Q),
   lager:warning("connect and Query: ~p",[Q]),
   ColumnNames = columns(Columns, []),
   lager:notice("ColumnName: ~p",[ColumnNames]),
   Batch = to_flowdata(ColumnNames, Rows),
   lager:notice("Batch:: ~p",[Batch]),
   State#state{client = C}.

columns([], ColumnNames) ->
   lists:reverse(ColumnNames);
columns([{column, Name, _Type, _, _, _, _}|RestC], ColumnNames) ->
   columns(RestC, [Name|ColumnNames]).

to_flowdata(Columns, ValueRows) ->
   VRows = [tuple_to_list(VRow) || VRow <- ValueRows],
   to_flowdata(Columns, lists:reverse(VRows), #data_batch{}).

to_flowdata(_C, [], Batch=#data_batch{}) ->
   Batch;
to_flowdata([<<"ts">>|Columns]=C, [[Ts|ValRow]|Values], Batch=#data_batch{points = Points}) ->
   Point = row_to_datapoint(Columns, ValRow, #data_point{ts = Ts}),
   to_flowdata(C, Values, Batch#data_batch{points = [Point|Points]}).

row_to_datapoint([], [], Point) ->
   Point;
row_to_datapoint([C|Columns], [Val|Row], Point) ->
   P = flowdata:set_field(Point, C, Val),
   row_to_datapoint(Columns, Row, P).
