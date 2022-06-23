%% Date: 06.04.20 - 08:27
%% Ⓒ 2020 heyoka
%% behaviour for mqtt event handlers
-module(event_handler_mqtt).
-author("Alexander Minichmair").

-behaviour(gen_event).

-include("faxe.hrl").

%% API

%% gen_event callbacks
-export([init/1,
   handle_event/2,
   handle_call/2,
   handle_info/2,
   terminate/2,
   code_change/3]).

-define(SERVER, ?MODULE).
-define(TOPIC_BASE, <<"ttgw/sys/faxe/">>).

-record(state, {
   publisher,
   publisher_opts,
   topic,
   cb,
   cb_state
}).

%%%===================================================================
%%% behaviour
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-callback init(Topic :: binary()) -> {ok, State :: term()}.
-callback handle_event(Event :: term(), State :: term()) ->
   {ok, State :: term()} | {publish, Topic :: binary(), Data :: term(), NewState :: term()}.


%%%===================================================================
%%% gen_event callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a new event handler is added to an event manager,
%% this function is called to initialize the event handler.
%%
%% @end
%%--------------------------------------------------------------------
-spec(init(InitArgs :: term()) ->
   {ok, State :: #state{}} |
   {ok, State :: #state{}, hibernate} |
   {error, Reason :: term()}).
init([Callback, Args]) ->
   MqttOpts1 = maps:from_list(Args),
   MqttOpts0 = MqttOpts1#{retained => true,  qos => 1},
   %% get the device name
   Name = faxe_util:device_name(),
   BaseTopic = proplists:get_value(base_topic, Args, ?TOPIC_BASE),
   Topic = faxe_util:build_topic([BaseTopic, Name]),
   {ok, AdditionalOpts, CbState} = Callback:init(Topic),
   MqttOpts = maps:merge(MqttOpts0, AdditionalOpts),
%%   lager:debug("mqtt-opts for callback: ~p :: ~p",[Callback, MqttOpts]),
   {ok, Publisher} = mqtt_publisher:start_link(MqttOpts),
   {ok,
      #state{publisher = Publisher, publisher_opts = MqttOpts,
         topic = Topic, cb = Callback, cb_state = CbState}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever an event manager receives an event sent using
%% gen_event:notify/2 or gen_event:sync_notify/2, this function is
%% called for each installed event handler to handle the event.
%%
%% @end
%%--------------------------------------------------------------------
-spec(handle_event(Event :: term(), State :: #state{}) ->
   {ok, NewState :: #state{}} |
   {ok, NewState :: #state{}, hibernate} |
   {swap_handler, Args1 :: term(), NewState :: #state{},
      Handler2 :: (atom() | {atom(), Id :: term()}), Args2 :: term()} |
   remove_handler).
handle_event(Event, State = #state{cb = Callback, cb_state = CbState, publisher = Publisher}) ->
%%   lager:notice("~p handle event: ~p",[{?MODULE, Callback}, Event]),
   NewCbState =
   case Callback:handle_event(Event, CbState) of
      {ok, CbState0} ->
         CbState0;
      {publish, Topic, Data, NewCbState1} ->
         publish(Topic, Data, Publisher),
         NewCbState1;
      _ -> CbState
   end,
   {ok, State#state{cb_state = NewCbState}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever an event manager receives a request sent using
%% gen_event:call/3,4, this function is called for the specified
%% event handler to handle the request.
%%
%% @end
%%--------------------------------------------------------------------
-spec(handle_call(Request :: term(), State :: #state{}) ->
   {ok, Reply :: term(), NewState :: #state{}} |
   {ok, Reply :: term(), NewState :: #state{}, hibernate} |
   {swap_handler, Reply :: term(), Args1 :: term(), NewState :: #state{},
      Handler2 :: (atom() | {atom(), Id :: term()}), Args2 :: term()} |
   {remove_handler, Reply :: term()}).
handle_call(_Request, State) ->
   Reply = ok,
   {ok, Reply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called for each installed event handler when
%% an event manager receives any other message than an event or a
%% synchronous request (or a system message).
%%
%% @end
%%--------------------------------------------------------------------
-spec(handle_info(Info :: term(), State :: #state{}) ->
   {ok, NewState :: #state{}} |
   {ok, NewState :: #state{}, hibernate} |
   {swap_handler, Args1 :: term(), NewState :: #state{},
      Handler2 :: (atom() | {atom(), Id :: term()}), Args2 :: term()} |
   remove_handler).
handle_info({publish, Topic, Data} = M, State = #state{publisher = Publisher}) ->
   publish(Topic, Data, Publisher),
   {ok, State};
handle_info({'EXIT', OldPublisher, _Reason}, State = #state{publisher = OldPublisher, publisher_opts = Opts}) ->
   %% publisher died
   {ok, Publisher} = mqtt_publisher:start_link(Opts),
   {ok, State#state{publisher = Publisher}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever an event handler is deleted from an event manager, this
%% function is called. It should be the opposite of Module:init/1 and
%% do any necessary cleaning up.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
-spec(terminate(Args :: (term() | {stop, Reason :: term()} | stop |
remove_handler | {error, {'EXIT', Reason :: term()}} |
{error, term()}), State :: term()) -> term()).
terminate(_Arg, #state{publisher = P}) ->
   catch gen_server:stop(P).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @end
%%--------------------------------------------------------------------
-spec(code_change(OldVsn :: term() | {down, term()}, State :: #state{},
    Extra :: term()) ->
   {ok, NewState :: #state{}}).
code_change(_OldVsn, State, _Extra) ->
   {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
publish(T, Item, Publisher) when is_record(Item, data_point) orelse is_record(Item, data_batch) ->
   publish(T, flowdata:to_json(Item), Publisher);
publish(T, Item, Publisher) when is_binary(Item) ->
   Publisher ! {publish, {T, Item}}.