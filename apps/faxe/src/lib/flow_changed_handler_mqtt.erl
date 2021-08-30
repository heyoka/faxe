%% Date: 27.08.21 - 08:27
%% Ⓒ 2021 heyoka
-module(flow_changed_handler_mqtt).
-author("Alexander Minichmair").

-behaviour(event_handler_mqtt).

-include("faxe.hrl").

%% event_handler_mqtt callbacks
-export([
   init/1,
   handle_event/2]).

-record(state, {
   topic,
   topic_flow_export,
   topic_template_export
}).

%%%===================================================================
%%% event_handler_mqtt callbacks
%%%===================================================================

-spec(init(InitArgs :: term()) ->
   {ok, Opts :: map(), State :: #state{}} |
   {ok, Opts :: map(), State :: #state{}, hibernate} |
   {error, Reason :: term()}).
init(Topic0) ->
   %% topic not used for now, as we only do flow/template export at the moment
   Topic = faxe_util:build_topic([Topic0, <<"flow_changed">>]),
   TopicExport = faxe_util:build_topic([Topic0, <<"flows_export">>]),
   TopicTemplateExport = faxe_util:build_topic([Topic0, <<"templates_export">>]),
   {ok, #{qos => 0, retained => true},
      #state{topic = Topic, topic_flow_export = TopicExport, topic_template_export = TopicTemplateExport}}.

handle_event(Event = #data_point{},
    State = #state{topic_template_export = TTempExport, topic_flow_export = TopicExport}) ->
   {Topic, Maps} =
      case flowdata:field(Event, <<"type">>) of
             template -> {TTempExport, [rest_helper:template_to_map(T) || T <- faxe:list_templates()]};
             _ -> {TopicExport, [rest_helper:task_to_map(T) || T <- faxe:list_tasks()]}
      end,
   {publish, Topic, #data_point{ts = faxe_time:now(), fields = #{export => Maps}}, State}.