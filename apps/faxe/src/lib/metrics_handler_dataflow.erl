%% Date: 06.04.20 - 08:27
%% Ⓒ 2020 heyoka
-module(metrics_handler_dataflow).
-author("Alexander Minichmair").

-behaviour(gen_event).

%% API
-export([start_link/1,
   add_handler/1]).

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
   parent,
   flow_id,
   node_id,
   metrics_list
}).

%%%===================================================================
%%% gen_event callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Creates an event manager
%%
%% @end
%%--------------------------------------------------------------------
-spec(start_link(atom()) -> {ok, pid()} | {error, {already_started, pid()}}).
start_link(RegName) ->
   Ret = gen_event:start_link({local, RegName}),
   Ret.

%%--------------------------------------------------------------------
%% @doc
%% Adds an event handler
%%
%% @end
%%--------------------------------------------------------------------
-spec(add_handler(EventMgrName :: atom()) -> ok | {'EXIT', Reason :: term()} | term()).
add_handler(EventMgrName) ->
   gen_event:add_sup_handler(EventMgrName, ?MODULE, []).

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
init(#{parent := Parent, flow_id := FlowId} = Opts) ->
   NodeId =
   case maps:is_key(node_id, Opts) of
      true -> maps:get(node_id, Opts);
      false -> undefined
   end,
   Metrics =
   case maps:is_key(metrics, Opts) of
      true -> maps:get(metrics, Opts);
      false -> undefined
   end,
   {ok, #state{parent = Parent, flow_id = FlowId, node_id = NodeId, metrics_list = Metrics}}.

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
handle_event({{FlowId}, Item} = _P, State = #state{node_id = undefined, flow_id = FlowId, parent = Parent}) ->
   Parent ! {flow_metric, Item},
%%   lager:info("metrics dataflow handler :: ~p",[P]),
   {ok, State};
handle_event({{FlowId, NodeId, MetricName} = _P, Item},
    State = #state{flow_id = FlowId, node_id = NodeId, metrics_list = Ms, parent = Parent}) ->
%%   lager:info("node metrics dataflow handler :: ~p",[P]),
    case Ms == undefined orelse lists:member(MetricName, Ms) of
       true -> Parent ! {node_metric, Item};
       false -> ok
    end,
   {ok, State};
handle_event(_, State) ->
   {ok, State}.

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
handle_info(_Info, State) ->
   {ok, State}.

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
terminate(_Arg, #state{ }) ->
   ok.

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