%% Date: 06.04.20 - 08:27
%% Ⓒ 2020 heyoka
-module(debug_handler_mqtt).
-author("Alexander Minichmair").

-behaviour(event_handler_mqtt).

%% event_handler_mqtt callbacks
-export([
   init/1,
   handle_event/2]).

-record(state, {
   topic
}).

%%%===================================================================
%%% event_handler_mqtt callbacks
%%%===================================================================

-spec(init(InitArgs :: term()) ->
   {ok, State :: #state{}} |
   {ok, State :: #state{}, hibernate} |
   {error, Reason :: term()}).
init(Topic0) ->
   Topic = <<Topic0/binary, "debug">>,
   {ok, #state{topic = Topic}}.


handle_event({Key, {FlowId, NodeId} = _FNId, Port, Item0}, State = #state{topic = Topic}) ->
   K = atom_to_binary(Key, utf8),
   T = <<Topic/binary, "/", FlowId/binary, "/", NodeId/binary, "/", K/binary>>,
   Item = flowdata:set_fields(Item0, [<<"meta.type">>, <<"meta.flow_id">>, <<"meta.node_id">>, <<"meta.port">>],
      [K, FlowId, NodeId, Port]),
%%   lager:info("DEBUG [~p] :: ~p on Port ~p~n~s",[FNId, Key, Port, flowdata:to_json(Item)]),
   {publish, T, Item, State}.
