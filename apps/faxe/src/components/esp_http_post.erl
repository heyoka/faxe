%% Date: 30.12.16 - 23:01
%% HTTP Post Request
%% Ⓒ 2019 heyoka
%%
-module(esp_http_post).
-author("Alexander Minichmair").

-include("faxe.hrl").

-behavior(df_component).
%% API
-export([init/3, process/3, options/0]).

-record(state, {
   host :: string(),
   port :: non_neg_integer(),
   path :: string(),
   key :: string(),
   client
}).

options() ->
   [
      {host, string},
      {port, integer},
      {path, string, <<"">>},
      {key, string}].

%%check_options() ->
%%   [{not_empty, [file]}].

init(_NodeId, _Inputs, #{host := Host0, port := Port, path := Path, key := Key}) ->
   Host = binary_to_list(Host0)++":"++integer_to_list(Port),
   lager:notice("host part is: ~p" ,[Host]),
   {ok, C} = fusco:start(Host, []),
   {ok, all, #state{host = Host, port = Port, path = Path, key = Key, client = C}}.

process(_In, P = #data_point{}, State = #state{}) ->
   send(P, State),
   {ok, State};
process(_In, B = #data_batch{}, State = #state{}) ->
   send(B, State),
   {ok, State}.

send(Item, #state{host = Host, port = Port, path = Path, key = Key, client = Client}) ->
   M = flowdata:to_mapstruct(Item),
   #{<<"ts">> := Ts, <<"df">> := Df, <<"vs">> := Vs, <<"data">> := Data, <<"id">> := Id} = M,
   Stmt = <<"INSERT INTO robotplc (ts, id, df, vs, data_obj) VALUES(?,?,?,?,?)">>,
   BulkArgs = [Ts, Id, Df, Vs, Data],
%%   lager:notice("mapstruct: ~p", [M]),
   Query = #{Key => Stmt, <<"args">> => BulkArgs},
%%   Url = Host ++ "/" ++ Path,
%%   Response =
%%   httpc:request(post,
%%      {Url, [],
%%         "application/json",
%%         jiffy:encode(Query)
%%      }, [], []),
   Response = fusco:request(Client, Path, "POST", [], jiffy:encode(Query), 10000),
   lager:notice("Query: ~s :: Response :: ~p",[jiffy:encode(Query), Response]),
%%   io:format(Host, "~s~n", [binary_to_list(flowdata:to_json(Item))]).
ok.