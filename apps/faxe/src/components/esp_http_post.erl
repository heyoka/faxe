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
   client
}).

options() ->
   [
      {host, string},
      {port, integer},
      {path, string, <<"">>}
   ].

init(_NodeId, _Inputs, #{host := Host0, port := Port, path := Path}) ->
   Host = binary_to_list(Host0)++":"++integer_to_list(Port),
   lager:notice("host part is: ~p" ,[Host]),
   {ok, C} = fusco:start(Host, []),
   {ok, all, #state{host = Host, port = Port, path = Path, client = C}}.

process(_In, P = #data_point{}, State = #state{}) ->
   send(P, State),
   {ok, State};
process(_In, B = #data_batch{}, State = #state{}) ->
   send(B, State),
   {ok, State}.

send(Item, #state{client = Client, path = Path}) ->
   M = flowdata:to_mapstruct(Item),
   Response = fusco:request(Client, Path, "POST", [], jiffy:encode(M), 10000),
   lager:notice("Query: ~s :: Response :: ~p",[jiffy:encode(M), Response]).