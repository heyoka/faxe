%% Date: 28.12.16 - 18:17
%% Ⓒ 2016 heyoka
-module(df_component).
-author("Alexander Minichmair").

-include("faxe.hrl").

-behaviour(gen_server).

%% API
-export([start_link/6]).
-export([start_node/4, inports/1, outports/1, start_async/4]).

%% Callback API
%%-export([request_items/2, emit/1, emit/2]).

%% gen_server callbacks
-export([init/1,
   handle_call/3,
   handle_cast/2,
   handle_info/2,
   terminate/2,
   code_change/3]).


-define(MSG_Q_LENGTH_HIGH_WATERMARK, 15).

-type auto_request()    :: 'all' | 'emit' | 'none'.

-type df_port()         :: non_neg_integer().


%%%===================================================================
%%% CALLBACKS
%%%===================================================================

%% @doc
%% INIT/3
%%
%% initialisation
%%
-callback init(NodeId :: term(), Inputs :: list(), Args :: term())
       -> {ok, auto_request(), cbstate()}.



%% @doc
%% PROCESS/2
%%
%% process value or batch incoming on specific inport
%%
%% return values :
%%
%% :: just return the state
%% {ok, cbstate()}
%%
%% :: used to emit a value right after processing
%% {emit, Port :: port(), Value :: term(), cbstate()} :: used emit right after processing
%%
%% :: request a value in return of a process call
%% {request, ReqPort :: port(), ReqPid :: pid(), cbstate()}
%%
%% :: emit and request a value
%% {emit_request, OutPort :: port(), Value :: term(), ReqPort :: port(), ReqPid :: pid(), cbstate()}
%% @end
%%
-callback process(Inport :: non_neg_integer(), Value :: #data_point{} | #data_batch{}, State :: cbstate())
       ->
       {ok, cbstate()} |

       {emit,
          { Port :: df_port(), Value :: term() }, cbstate()
       } |
       {request,
          { ReqPort :: df_port(), ReqPid :: pid() }, cbstate()
       } |
       {emit_request,
          { OutPort :: df_port(), Value :: term() }, { ReqPort :: df_port(), ReqPid :: pid() }, cbstate()
       } |

       {error, Reason :: term()}.


%%%==========================================================
%%% OPTIONAL CALLBACKS
%%% =========================================================

%% @doc
%% OPTIONS/0
%%
%% optional
%%
%% retrieve options (with default values optionally) for a component
%% for an optional parameter, provide Default term
%%
%% options with no 'Default' value, will be treated as mandatory
%% @end
 -callback options() ->
 list(
    {Name :: atom(), Type :: dataflow:option_value(), Default :: dataflow:option_value()} |
    {Name :: atom(), Type :: atom()}
 ).
%% @end

%% @doc
%% further checks for options, see dataflow module
%% optional
%% @end
-callback check_options() ->
   list(
      {Type :: atom(), term()} | {Type :: atom(), term(), term()} | {Type :: atom(), term(), term(), term()}
   ).

%% @doc
%% INPORTS/0
%%
%% optional
%% provide a list of inports for the component :
%%
 -callback inports()  -> {ok, list()}.



%% @doc
%% OUTPORTS/0
%%
%% optional
%% provide a list of outports for the component:
%%
 -callback outports() -> {ok, list()}.



%% @doc
%% HANDLE_INFO/2
%%
%% optional
%% handle other messages that will be sent to this process :
%%
%% @end
 -callback handle_info(Request :: term(), State :: cbstate())
 -> {ok, NewCallbackState :: cbstate()} | {error, Reason :: term()}.


%% @doc
%% SHUTDOWN/1
%%
%% optional
%% called when component process is about to stop :
%%
%% @end
-callback shutdown(State :: cbstate())
    -> any().

%% these are optional
-optional_callbacks([
   options/0, check_options/0,
   inports/0, outports/0,
   handle_info/2, shutdown/1]). %% erlang 18+



%%%===================================================================
%%% API
%%%===================================================================

-spec(start_link(atom(), term(), term(), list(), list(), term()) ->
   {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link(Component, GraphId, NodeId, Inports, Outports, Args) ->
   gen_server:start_link(?MODULE, [Component, GraphId, NodeId, Inports, Outports, Args], []).

start_node(Server, Inputs, Subscriptions, FlowMode) ->
   gen_server:call(Server, {start, Inputs, Subscriptions, FlowMode}).

start_async(Server, Inputs, Subscriptions, FlowMode) ->
   Server ! {start, Inputs, Subscriptions, FlowMode}.

inports(Module) ->
   case erlang:function_exported(Module, inports, 0) of
      true -> Module:inports();
      false -> inports()
   end.

outports(Module) ->
   case erlang:function_exported(Module, outports, 0) of
      true -> Module:outports();
      false -> outports()
   end.


%%%==========================================================
%%% Callback API
%%%
%%% these are exposed in the dataflow module now !
%%%==========================================================
%%request_items(Port, PublisherPids) when is_list(PublisherPids) ->
%%   [Pid ! {request, self(), Port} || Pid <- PublisherPids].
%%
%%emit(Value) ->
%%   emit(1, Value).
%%emit(Port, Value) ->
%%   erlang:send_after(0, self(), {emit, {Port, Value}}).


%%% %%%%%%%%%%%%%%%%
%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

-spec(init(Args :: term()) ->
   {ok, State :: #c_state{}} | {ok, State :: #c_state{}, timeout() | hibernate} |
   {stop, Reason :: term()} | ignore).
init([Component, GraphId, NodeId, Inports, _Outports, Args]) ->
   code:ensure_loaded(Component),
   lager:md([{flow, GraphId}, {comp, NodeId}]),
   InputPorts = lists:map(fun({_Pid, Port}) -> Port end, Inports),
   {ok, #c_state{component = Component, graph_id = GraphId, node_id = NodeId, subscriptions = [],
      inports = InputPorts, cb_state = Args}};
init(#c_state{} = PersistedState) ->
   {ok, PersistedState}.


handle_call({start, Inputs, Subscriptions, FlowMode}, _From,
    State=#c_state{component = CB, cb_state = CBState, node_id = NId}) ->

   %gen_event:notify(dfevent_component, {start, State#c_state.node_id, FlowMode}),
   lager:debug("component ~p starts with options; ~p", [CB, CBState]),
   Opts = CBState,
   Inited = CB:init(NId, Inputs, Opts),
   {AReq, NewCBState} =
   case Inited of

      {ok, ARequest, NCBState}      -> {ARequest, NCBState};
      {ok, NCBState}                -> {all, NCBState};
      {error_options, Message}      -> erlang:error(Message);
      {error, What}                 -> erlang:error(What)
   end,

   AR = case FlowMode of pull -> AReq; push -> none end,
   CallbackHandlesInfo = erlang:function_exported(CB, handle_info, 2),
   %% metrics
%%   folsom_metrics:new_histogram(NId, slide, 60),
%%   folsom_metrics:new_history(<< NId/binary, ?FOLSOM_ERROR_HISTORY >>, 24),
   {reply, ok,
      State#c_state{
         subscriptions = Subscriptions,
         inports = Inputs,
         auto_request = AR,
         cb_state = NewCBState,
         flow_mode = FlowMode,
         cb_handle_info = CallbackHandlesInfo}}
;
handle_call(stats, _From, State=#c_state{node_id = _NId, component = _Comp}) ->
%%   Res = {Comp,#{
%%      <<"processing_errors">> => folsom_metrics:get_history_values(<< NId/binary, ?FOLSOM_ERROR_HISTORY >>, 24),
%%      <<"items processed">> => folsom_metrics:get_histogram_statistics(NId)
%%   }},
   {reply, ok, State}
;
handle_call(errors, _From, State=#c_state{node_id = _NId, component = _Comp}) ->
%%   Res = {Comp, folsom_metrics:get_history_values(<< NId/binary, ?FOLSOM_ERROR_HISTORY >>, 24)},
   {reply, ok, State}
;
handle_call(_What, _From, State) ->
   lager:info("~p: unexpected handle_call with ~p",[?MODULE, _What]),
   {reply, error, State}
.

handle_cast(_Request, State) ->
   {noreply, State}.


%% @doc
%% start the node asynchronously
%% these are the messages from and to other dataflow nodes
%% do not use these tags in your callback 'handle_info' functions :
%% 'start' | 'request' | 'item' | 'emit' | 'pull' | 'stop'
%% you will not receive the info message in the callback with these
%%
%% @end
handle_info({start, Inputs, Subscriptions, FlowMode},
    State=#c_state{component = CB, cb_state = CBState, node_id = NId}) ->

   %gen_event:notify(dfevent_component, {start, State#c_state.node_id, FlowMode}),
   lager:debug("component ~p starts with options; ~p", [CB, CBState]),
   Opts = CBState,
   Inited = CB:init(NId, Inputs, Opts),
   {AReq, NewCBState} =
      case Inited of

         {ok, ARequest, NCBState}      -> {ARequest, NCBState};
         {ok, NCBState}                -> {all, NCBState};
         {error_options, Message}      -> erlang:error(Message);
         {error, What}                 -> erlang:error(What)
      end,

   AR = case FlowMode of pull -> AReq; push -> none end,
   CallbackHandlesInfo = erlang:function_exported(CB, handle_info, 2),
   %% metrics
%%   folsom_metrics:new_histogram(NId, slide, 60),
%%   folsom_metrics:new_history(<< NId/binary, ?FOLSOM_ERROR_HISTORY >>, 24),
   {noreply,
      State#c_state{
         subscriptions = Subscriptions,
         inports = Inputs,
         auto_request = AR,
         cb_state = NewCBState,
         flow_mode = FlowMode,
         cb_handle_info = CallbackHandlesInfo}}
;


handle_info({request, ReqPid, ReqPort}, State=#c_state{subscriptions = Ss}) ->
   NewSubs = df_subscription:request(Ss, ReqPid, ReqPort),
   {noreply, State#c_state{subscriptions =  NewSubs}};

handle_info({item, {Inport, Value}},
    State=#c_state{
       cb_state = CBState, component = Module, flow_mode = FMode, auto_request = AR, node_id = _NId}) ->

   {message_queue_len, MsgQueueLength} = erlang:process_info(self(), message_queue_len),
   case MsgQueueLength > ?MSG_Q_LENGTH_HIGH_WATERMARK of
      true -> lager:warning("[~p] Process-Q-Length: ~p", [Module, MsgQueueLength]);
      false -> ok
   end,
%%
%%   lager:notice("stats for ~p: ~p",[NId, folsom_metrics:get_histogram_statistics(NId)]),
%%lager:warning("cb state is: ~p",[CBState]),
%%   folsom_metrics:notify({NId, 1}),
   %gen_event:notify(dfevent_component, {item, State#c_state.node_id, {Inport, Value}}),
%%   Result = (Module:process(Inport, Value, CBState)),
   case  catch(Module:process(Inport, Value, CBState)) of
      {'EXIT', {Reason,Stacktrace}} ->
         lager:error("'error' ~p in component ~p caught when processing item: ~p -- ~p",
         [Reason, State#c_state.component, {Inport, Value}, Stacktrace]),
%%         folsom_metrics:notify({<< NId/binary, ?FOLSOM_ERROR_HISTORY >>,
%%%%            io_lib:format("'error' ~p in component ~p caught when processing item: ~p -- ~p",
%%            [time_format:to_iso8601(faxe_time:now()), Reason, State#c_state.component, {Inport, Value}, Stacktrace]
%%%%         )}
%%         }),
         {noreply, State};

      Result ->
         {NewState, Requested, REmitted} = handle_process_result(Result, State),
         case FMode == pull of
            true -> case {Requested, AR, REmitted} of
                       {true, _, _} -> ok;
                       {false, none, _} -> ok;
                       {false, emit, false} -> ok;
                       {false, emit, true} -> request_all(State#c_state.inports, FMode);
                       {false, all, _} -> request_all(State#c_state.inports, FMode)
                    end;
            false -> ok
         end,
%%         handle_ls_mem(Value, State),
         {noreply, NewState}
   end
   ;

handle_info({emit, {Outport, Value}}, State=#c_state{subscriptions = Ss, node_id = _NId,
      flow_mode = FMode, auto_request = AR, emitted = EmitCount}) ->

%%   lager:notice("stats for ~p: ~p",[NId, folsom_metrics:get_histogram_statistics(NId)]),
   %gen_event:notify(dfevent_component, {emitting, State#c_state.node_id, {Outport, Value}}),
   lager:debug("Component: ~p emitting: ~p on port ~p", [State#c_state.node_id, Value, Outport]),

   NewSubs = df_subscription:output(Ss, Value, Outport),
   NewState = State#c_state{subscriptions = NewSubs},
   case AR of
      none  -> ok;
      _     -> request_all(State#c_state.inports, FMode)
   end,
   {noreply, NewState#c_state{emitted = EmitCount+1}};

handle_info(pull, State=#c_state{inports = Ins}) ->
   lists:foreach(fun({Port, Pid}) -> dataflow:request_items(Port, [Pid]) end, Ins),
   {noreply, State}
;
handle_info(stop, State=#c_state{node_id = _N, component = Mod, cb_state = CBState}) ->

   %gen_event:notify(dfevent_component, {stopping, N, Mod}),
   case erlang:function_exported(Mod, shutdown, 1) of
      true -> Mod:shutdown(CBState);
      false -> ok
   end,
   {stop, normal, State}
;
handle_info(Req, State=#c_state{component = Module, cb_state = CB, cb_handle_info = true}) ->
   NewCB = case Module:handle_info(Req, CB) of
              {ok, CB0} -> CB0;
              {error, _Reason} -> error
           end,
   {noreply, State#c_state{cb_state = NewCB}}
;
handle_info(_Req, State=#c_state{cb_handle_info = false}) ->
   {noreply, State}
.

%%--------------------------------------------------------------------
-spec(terminate(Reason :: (normal | shutdown | {shutdown, term()} | term()),
    State :: #c_state{}) -> term()).
terminate(_Reason, _State) ->
   ok.


-spec(code_change(OldVsn :: term() | {down, term()}, State :: #c_state{},
    Extra :: term()) ->
   {ok, NewState :: #c_state{}} | {error, Reason :: term()}).
code_change(_OldVsn, State, _Extra) ->
   {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec handle_process_result(tuple(), #c_state{}) -> {NewState::#c_state{}, boolean(), boolean()}.
handle_process_result({emit, {Port, Emitted}, NState}, State=#c_state{subscriptions = Subs}) when is_integer(Port) ->
   NewSubs = df_subscription:output(Subs, Emitted, Port),
   {State#c_state{subscriptions = NewSubs, cb_state = NState},false, true};
handle_process_result({emit, Emitted, NState}, State=#c_state{subscriptions = Subs}) ->
   NewSubs = df_subscription:output(Subs, Emitted, 1),
   {State#c_state{subscriptions = NewSubs, cb_state = NState},false, true};
handle_process_result({request, {Port, PPids}, NState}, State=#c_state{flow_mode = FMode}) when is_list(PPids) ->
   maybe_request_items(Port, PPids, FMode),
   {State#c_state{cb_state = NState},
      true, false};
handle_process_result({emit_request, {Port, Emitted}, {ReqPort, PPids}, NState},
    State=#c_state{flow_mode = FMode, subscriptions = Subs}) when is_list(PPids) ->
   NewSubs = df_subscription:output(Subs, Emitted, Port),
   maybe_request_items(ReqPort, PPids, FMode),
   {State#c_state{subscriptions = NewSubs, cb_state = NState}, true, false};
handle_process_result({emit_request, Emitted, {ReqPort, PPids}, NState},
    State=#c_state{flow_mode = FMode, subscriptions = Subs}) when is_list(PPids) ->
   NewSubs = df_subscription:output(Subs, Emitted, 1),
   maybe_request_items(ReqPort, PPids, FMode),
   {State#c_state{subscriptions = NewSubs, cb_state = NState}, true, false};
handle_process_result({ok, NewCBState}, State=#c_state{}) ->
   {State#c_state{cb_state = NewCBState}, false, false};
handle_process_result({error, _What}, _State=#c_state{}) ->
   exit(_What).



request_all(_Inports, push) ->
   ok;
request_all(Inports, pull) ->
   {_Ports, Pids} = lists:unzip(Inports),
   maybe_request_items(all, Pids, pull).
maybe_request_items(_Port, _Pids, push) ->
   ok;
maybe_request_items(Port, Pids, pull) ->
   dataflow:request_items(Port, Pids).


%%%===================================================================
%%% PORTS for modules
%%%

inports() ->
   [{1, nil}].

outports() ->
   inports().

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%