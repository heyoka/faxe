%% Date: 06.04.20 - 08:27
%% Ⓒ 2020 heyoka
-module(conn_status_handler_mqtt).
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
   Topic = faxe_util:build_topic([Topic0, <<"conn_status">>]),
   {ok, #{retained => true, qos => 1}, #state{topic = Topic}}.

handle_event({{FlowId, NodeId}, Item}, State = #state{topic = Topic}) ->
   T = <<Topic/binary, "/", FlowId/binary, "/", NodeId/binary>>,
   {publish, T, Item, State}.
