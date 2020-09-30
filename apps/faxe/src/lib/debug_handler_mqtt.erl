%% Date: 06.04.20 - 08:27
%% Ⓒ 2020 heyoka
-module(debug_handler_mqtt).
-author("Alexander Minichmair").

-behaviour(event_handler_mqtt).

-include("faxe.hrl").
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
   Topic = faxe_util:build_topic([Topic0, <<"debug">>]),
   {ok, #state{topic = Topic}}.

handle_event({Key, {FlowId, NodeId} = _FNId, Port, Item}, State = #state{topic = Topic}) ->
   K = atom_to_binary(Key, utf8),
   T = <<Topic/binary, "/", FlowId/binary, "/", NodeId/binary, "/", K/binary>>,
   Out0 = #data_point{ts = faxe_time:now(), fields = #{<<"data_item">> => flowdata:to_json(Item)}},
   Out = flowdata:set_fields(Out0,
      [<<"meta.type">>, <<"meta.flow_id">>, <<"meta.node_id">>, <<"meta.port">>],
      [K, FlowId, NodeId, Port]),
%%   lager:info("DEBUG [~p] :: ~p on Port ~p~n~s~n~s",[_FNId, Key, Port, flowdata:to_json(Out), T]),
   {publish, T, Out, State}.
