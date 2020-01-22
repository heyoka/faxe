%% Date: 30.12.16 - 23:01
%% HTTP Post Request
%% Ⓒ 2019 heyoka
%%
%% @todo retry on error
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

-define(HTTP_PROTOCOL, <<"http://">>).

options() ->
   [
      {host, string},
      {port, integer},
      {path, string, <<"">>}
   ].

init(_NodeId, _Inputs, #{host := Host0, port := Port, path := Path}) ->
   Host1 = faxe_util:host_with_protocol(Host0, ?HTTP_PROTOCOL),
   Host = binary_to_list(Host1)++":"++integer_to_list(Port),
   {ok, C} = fusco:start(Host, []),
   {ok, all, #state{host = Host, port = Port, path = Path, client = C}}.

process(_In, P = #data_point{}, State = #state{}) ->
   send(P, State),
   {ok, State};
process(_In, B = #data_batch{}, State = #state{}) ->
   send(B, State),
   {ok, State}.

send(Item, #state{client = Client, host = H, path = Path}) ->
   M = flowdata:to_mapstruct(Item),
   Response = fusco:request(Client, Path, "POST", [], jiffy:encode(M), 10000),
   case Response of
      {error, What} ->
         lager:warning("error sending post request to ~p : ~p",
            [H++"/"++binary_to_list(Path), What]);
      _ -> lager:notice("Query: ~s :: Response :: ~p",[jiffy:encode(M), Response])
   end.
