%% Date: 30.12.16 - 23:01
%% HTTP Post Request
%% Ⓒ 2019 heyoka
%%
-module(esp_postgre_out).
-author("Alexander Minichmair").

-include("faxe.hrl").

-behavior(df_component).
%% API
-export([init/3, process/3, options/0, handle_info/2]).

-record(state, {
   host :: string(),
   port :: non_neg_integer(),
   user :: string(),
   pass :: string(),
   database,
   table,
   client,
   client_ref
}).

options() ->
   [
      {host, string},
      {port, integer},
      {user, string},
      {pass, string},
      {database, string},
      {table, string}
   ].

%%check_options() ->
%%   [{not_empty, [file]}].

init(_NodeId, _Inputs, #{host := Host, port := Port, user := User, pass := Pass, database := DB, table := Table}) ->
   State = #state{host = Host, port = Port, user = User, pass = Pass, database = DB, table = Table},
   NewState = connect(State),
   {ok, all, NewState}.

process(_In, P = #data_point{}, State = #state{}) ->
   send(P, State),
   {ok, State};
process(_In, B = #data_batch{}, State = #state{}) ->
   send(B, State),
   {ok, State}.

handle_info({_C, _Ref, connected}, State) ->
   %% connected
   lager:notice("epgsql connected!"),
   {ok, State};
handle_info({C, Ref, Error = {error, _}}, State) ->
   lager:error("~p Error: ~p", [?MODULE, Error]),
   {ok, State#state{client_ref = undefined, client = undefined}};
handle_info({'EXIT', _C, _Reason}, State) ->
   lager:notice("EXIT epgsql"),
   NewState = connect(State),
   {ok, NewState}.


send(Item, #state{client = C}) ->
   Query = "INSERT INTO table VALUES(ts, id, df, vs, data_obj)",
   Params = [],
   {ok, _Count, _Columns, _Rows} = epgsql:equery(C, Query, [Params]),
%%   io:format(Host, "~s~n", [binary_to_list(flowdata:to_json(Item))]).
ok.

connect(State = #state{host = Host, port = Port, user = User, pass = Pass, database = DB}) ->
   {ok, C} = epgsqla:start_link(),
   Ref = epgsqla:connect(C, #{host => Host, port => Port, username => User, pass => Pass, database => DB}),
   State#state{client = C, client_ref = Ref}.