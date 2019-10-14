%% Date: 30.12.16 - 23:01
%% HTTP Post Request
%% Ⓒ 2019 heyoka
%%
-module(esp_postgre_out).
-author("Alexander Minichmair").

-include("faxe.hrl").

-behavior(df_component).
%% API
-export([init/3, process/3, options/0, handle_info/2, check_options/0]).

-define(DB_OPTIONS, #{
   codecs => [{faxe_epgsql_codec, nil}],
   timeout => 5000
   }).

-record(state, {
   host :: string(),
   port :: non_neg_integer(),
   user :: string(),
   pass :: string(),
   database,
   table,
   client,
   client_ref,
   db_opts,
   db_fields = [],
   faxe_fields = [],
   query
}).

options() ->
   [
      {host, string},
      {port, integer},
      {user, string},
      {pass, string, <<>>},
      {database, string},
      {table, string},
      {db_fields, string_list, []},
      {faxe_fields, string_list, []}
   ].

check_options() ->
   [{same_length, [db_fields, faxe_fields]}].

init(_NodeId, _Inputs, #{host := Host0, port := Port, user := User, pass := Pass, database := DB,
   table := Table, db_fields := DBFields, faxe_fields := FaxeFields}) ->
   Host = binary_to_list(Host0),
   Opts = #{host => Host, port => Port, username => User, pass => Pass, database => DB},
   DBOpts = maps:merge(?DB_OPTIONS, Opts),
   State = #state{host = Host, port = Port, user = User, pass = Pass, database = DB, table = Table,
      db_opts = DBOpts},
   NewState = connect(State),
   Query = build_query(DBFields, Table),
   lager:notice("Query is : ~p", [Query]),
   {ok, all, NewState#state{db_fields = DBFields, faxe_fields = FaxeFields, query = Query}}.

process(_In, P = #data_point{}, State = #state{}) ->
   insert(P, State),
   {ok, State};
process(_In, B = #data_batch{}, State = #state{}) ->
   insert(B, State),
   {ok, State}.

handle_info({'EXIT', _C, _Reason}, State) ->
   lager:notice("EXIT epgsql"),
   NewState = connect(State),
   {ok, NewState};
handle_info(What, State) ->
   lager:warning("unexpected info: ~p",[What]),
   {ok, State}.

insert(Item, #state{client = C, faxe_fields = Fields, query = Q}) ->
   Vals = build_value_stmt(Item, Fields),
   Query = <<Q/binary, Vals/binary>>,
   lager:notice("QUERY : ~s",[Query]),
   Res = epgsql:equery(C, Query),
   lager:warning("Insert result: ~p", [Res]).


connect(State = #state{db_opts = Opts}) ->
   {ok, C} = epgsql:connect(Opts),
   State#state{client = C}.

build_query(ValueList0, Table) when is_list(ValueList0) ->
   Q0 = <<"INSERT INTO ", Table/binary>>,
   ValueList = [<<"ts">>|ValueList0],
   Fields = iolist_to_binary(lists:join(<<", ">>, ValueList)),
   Q = <<Q0/binary, " (", Fields/binary, ") VALUES">>,
   Q.

build_value_stmt(B = #data_batch{points = Points}, Fields) ->
   build_batch(Points, Fields, <<>>);
build_value_stmt(P = #data_point{ts = Ts}, Fields) ->
   DataList0 = flowdata:fields(P, Fields),
   DataList = [convert(V) || V <- [Ts|DataList0]],
   DataStmt = iolist_to_binary(lists:join(<<",">>, DataList)),
   Stmt = <<" (", DataStmt/binary, ")">>,
   Stmt.

build_batch([], _FieldList, Acc) ->
   Acc;
build_batch([Point|Points], FieldList, AccBin) ->
   NewBin = <<AccBin/binary, (build_value_stmt(Point, FieldList))>>,
   build_batch(Points, FieldList, NewBin).

convert(Val) when is_binary(Val) ->
   ["'", binary_to_list(Val), "'"];
convert(Val) when is_integer(Val) andalso Val > 255 ->
   integer_to_list(Val);
%%   list_to_binary(integer_to_list(Val));
convert(Val) when is_float(Val) ->
   list_to_binary(float_to_list(Val));
convert(Val) when is_map(Val) -> binary:replace(jiffy:encode(Val), <<":">>, <<"=">>, [global]);
convert(Val) -> Val.

