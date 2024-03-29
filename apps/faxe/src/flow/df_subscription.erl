%% Date: 28.12.16 - 19:43
%% Ⓒ 2016 heyoka
%%
%% @doc
%% This module works with #subscription records and handles out-routing of values
%% and in-routing of requests from other nodes (in order to handle push mode(s))
%%
%% this module will actually send messages to other df_component nodes in the graph
%%
-module(df_subscription).
-author("Alexander Minichmair").

-include("faxe.hrl").

%% API
-export([new/5, output/3, request/3, save_subscriptions/2, subscriptions/1, list/1]).

new(FlowMode, Publisher, PublisherPort, Subscriber, SubscriberPort) ->
   #subscription{
      flow_mode = FlowMode,
      publisher_pid = Publisher,
      publisher_port = PublisherPort,
      subscriber_pid = Subscriber,
      subscriber_port = SubscriberPort,
      out_buffer = queue:new()
   }.

-spec save_subscriptions({binary(), binary()}, list({non_neg_integer(), list(#subscription{})})) -> true.
save_subscriptions({_GraphId, _NodeId} = Index, Subscriptions) when is_list(Subscriptions) ->
   ets:insert(flow_subscriptions, {Index, Subscriptions}).

list({_GraphId, _NodeId} = Index) ->
   subscriptions(Index).

-spec subscriptions({binary(), binary()}) -> list({non_neg_integer(), list(#subscription{})}).
subscriptions({_GraphId, _NodeId} = Index) ->
   case ets:lookup(flow_subscriptions, Index) of
      [] -> [];
      [{Index, Subs}] -> Subs
   end;
subscriptions(GraphId) when is_binary(GraphId) ->
   case ets:match_object(flow_subscriptions, {{GraphId, '_'}, '$1'}) of
      '$end_of_table' -> [];
      Res -> Res
   end.

-spec output({binary(), binary()}, #data_point{}|#data_batch{}, non_neg_integer()) -> true.
output({_GraphId, _NodeId} = Index, Value, Port) ->
   NewSubscriptions = output(subscriptions(Index), Value, Port),
   save_subscriptions(Index, NewSubscriptions);
%% outputting a value on a specific out-port
output([], _Value, _Port) ->
   [];
output(Subscriptions, Value, Port) when is_list(Subscriptions) ->
%%   lager:info("SUBSCRIPTION OUTPUT: ~p",[{Port, lager:pr(Value,?MODULE)}]),
   Subs = proplists:get_value(Port, Subscriptions, []),
%%   lager:info("found subscriptions: ~p", [Subs]),
   NewSubs = lists:map(fun(X) -> output(X, Value) end, Subs),
   OSubs = proplists:delete(Port, Subscriptions),
   [{Port, NewSubs}| OSubs].

output(S = #subscription{flow_mode = push, subscriber_port = Port, subscriber_pid = SPid},
    Input) ->
   SPid ! {item, {Port, Input}},
   S
;
output(S = #subscription{flow_mode = pull, pending = false, out_buffer = Buffer}, Input) ->
   S#subscription{out_buffer = queue:in(Input, Buffer)}
;
output(S = #subscription{flow_mode = pull, pending = true, subscriber_pid = SPid,
   subscriber_port = Port}, Input) ->
   SPid ! {item, {Port, Input}},
   S#subscription{pending = false}
.

%% a value is requested from a subscriber node
request({_GraphId, _NodeId} = Index, Pid, Port) ->
   NewSubs = request(subscriptions(Index), Pid, Port),
   save_subscriptions(Index, NewSubs);
request(Subscriptions, Pid, Port) when is_list(Subscriptions) ->
   Subs = lists:map(fun({K, E}) -> {K, req_do(E, Pid, Port, [])} end, Subscriptions),
   Subs
;
request(S = #subscription{flow_mode = push}, _Pid, _Port) ->
   S;
request(S = #subscription{flow_mode = pull, subscriber_pid = SPid}, SPid, all) ->
   do_request(S);
request(S = #subscription{flow_mode = pull, subscriber_pid = SPid, subscriber_port = Port}, SPid, Port) ->
   do_request(S);
request(S = #subscription{flow_mode = pull}, _Pid, _Port) ->
   S.

do_request(S = #subscription{flow_mode = pull, out_buffer = Buffer, subscriber_pid = SPid,
      subscriber_port = Port, pending = _Pending}) ->
%%   io:format("Buffer when requesting value: ~p~n",[queue:len(Buffer)]),
   NewSubscription =
      case queue:out(Buffer) of
         {{value, Item }, Q2} -> SPid ! {item, {Port, Item}},
%%            io:format("item from buffer available: ~p~n",[{Port, Item}]),
            S#subscription{out_buffer = Q2, pending = false};
         {empty, _Q1} -> S#subscription{pending = true}
      end,
   NewSubscription.

req_do([], _Pid, _Port, Acc) ->
   Acc;
req_do([H|T], Pid, Port, Acc) ->
   req_do(T, Pid, Port, [request(H, Pid, Port)|Acc]).
