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

-include("dataflow.hrl").

%% API
-export([new/5, output/3, request/3]).

new(FlowMode, Publisher, PublisherPort, Subscriber, SubscriberPort) ->
   #subscription{
      flow_mode = FlowMode,
      publisher_pid = Publisher,
      publisher_port = PublisherPort,
      subscriber_pid = Subscriber,
      subscriber_port = SubscriberPort,
      out_buffer = queue:new()
   }.

%% outputting a value on a specific out-port
output(Subscriptions, Value, Port) when is_list(Subscriptions) ->
   Subs = get(Port, Subscriptions),
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
%%   io:format("Buffer when requesting value: ~p~n",[Buffer]),
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

get(Key, Subscriptions) ->
   proplists:get_value(Key, Subscriptions, []).