%%% @doc AMQP publishing-worker.
%%% @author Alexander Minichmair
%%%
-module(bunny_esq_worker).
-behaviour(gen_server).
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Required Types.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-include_lib("amqp_client/include/amqp_client.hrl").
-include("faxe.hrl").

-define(DELIVERY_MODE_NON_PERSISTENT, 1).
-define(DEQ_INTERVAL, 15).
-define(MAX_CHANNEL_NUMBER, 65535).

-record(state, {
   reconnector          = undefined,
   connection           = undefined :: undefined|pid(),
   channel              = undefined :: undefined|pid(),
   channel_ref          = undefined :: undefined|reference(),
   config               = []        :: proplists:proplist(),
   available            = false     :: boolean(),
   pending_acks         = #{}       :: map(),
   last_confirmed_dtag  = 0         :: non_neg_integer(),
   queue,
   deq_interval         = ?DEQ_INTERVAL,
   adaptive_interval                :: adaptive_interval:adaptive_interval(),
   deq_timer_ref,
   delivery_mode        = ?DELIVERY_MODE_NON_PERSISTENT,
   safe_mode            = false, %% whether to work with ondisc queue acks
   mem_q                            :: memory_queue:mem_queue()
}).

-type state():: #state{}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Exports.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Public API.
-export([start_link/2, stop/1, start/2]).

%%% gen_server/worker_pool callbacks.
-export([
   init/1, terminate/2, code_change/3,
   handle_call/3, handle_cast/2, handle_info/2
]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Public API.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec start_link(pid(), list()) -> any().
start_link(Queue, Args) ->
   gen_server:start_link(?MODULE, [Queue, Args], []).

start(Queue, Args) ->
   gen_server:start(?MODULE, [Queue, Args], []).

stop(Server) ->
%%   gen_server:stop(Server).
   Server ! stop.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% gen_server API.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec init(list()) -> {ok, state()}.
init([Queue, Config]) ->

   process_flag(trap_exit, true),
   Reconnector = faxe_backoff:new({100, 4200}),
   {ok, Reconnector1} = faxe_backoff:execute(Reconnector, connect),
   AmqpParams = amqp_options:parse(Config),
   SafeMode = maps:get(safe_mode, Config, false),
   DeliveryMode = case maps:get(persistent, Config, false) of true -> 2; false -> 1 end,
   MemQ = case Queue of undefined -> memory_queue:new(); _ -> undefined end,
   AdaptInt = adaptive_interval:new(),
%%   lager:info("adaptive interval: ~p",[lager:pr(AdaptInt, adaptive_interval)]),
   {ok, #state{
      reconnector = Reconnector1,
      queue = Queue,
      config = AmqpParams,
      deq_interval = adaptive_interval:current(AdaptInt),
      adaptive_interval = AdaptInt,
      delivery_mode = DeliveryMode,
      safe_mode = SafeMode,
      mem_q = MemQ}}.

-spec handle_cast(term(), state()) -> {noreply, state()}.
handle_cast(Msg, State) ->
   lager:warning("Invalid cast: ~p in ~p", [Msg, ?MODULE]),
   {noreply, State}.

%%%%%%%%%%%%%%%%%%%

-spec handle_info(term(), state()) -> {noreply, state()}.
handle_info(connect, State) ->
   lager:debug("[~p] connect to rmq",[?MODULE]),
   NewState = start_connection(State),
   case NewState#state.available of
      true ->
         NState = maybe_redeliver(NewState), {noreply, maybe_start_deq_timer(NState)};
      false -> {noreply, NewState}
   end;


handle_info( {'DOWN', _Ref, process, Conn, _Reason} = _Req, State=#state{connection = Conn}) ->
   lager:debug("RMQ Connection down, waiting for EXIT on channel ..."),
   {noreply, State};
handle_info({'EXIT', MQPid, Reason}, State=#state{channel = MQPid, reconnector = Recon} ) ->
   lager:notice("MQ channel DIED: ~p", [Reason]),
   {ok, Reconnector} = faxe_backoff:execute(Recon, connect),
   {noreply, State#state{
      reconnector = Reconnector,
      channel = undefined,
      channel_ref = undefined,
      available = false
   }};

%% dequeue
handle_info(deq, State = #state{available = false}) ->
   {noreply, State};
handle_info(deq, State = #state{available = true, channel = Channel}) ->
   case is_process_alive(Channel) of
      true ->
         {noreply, next(State)};
      false ->
         {noreply, State#state{available = false}}
   end;

handle_info({deliver, Exchange, Key, Payload, Args}, State = #state{channel = Ch}) ->
   Avail = is_pid(Ch) andalso erlang:is_process_alive(Ch),
   NewState = deliver({Exchange, Key, Payload, Args}, 1, State#state{available = Avail}),
   {noreply, NewState};

%% @doc
%% We handle the ack, nack, etc... - messages with these functions
%% the last delivery tag will be stored in #state
%% if the 'multiple' flag is set, the sequence is : |- from ('#state.last_confirmed_dtag' + 1) to DTag -|
%%
%% this function will release the given leases (delivery_tag(s)) in acking the stored esq-receipts, if in safe_mode
%% @end
handle_info(#'basic.ack'{}, State = #state{safe_mode = false}) ->
   {noreply, State};
handle_info(#'basic.ack'{delivery_tag = DTag, multiple = Multiple},
    State = #state{queue = Q, pending_acks = Pending}) ->
   Tags =
   case Multiple of
      true -> %lager:warning("RabbitMQ confirmed MULTIPLE Tags till ~p",[DTag]),
               lists:seq(State#state.last_confirmed_dtag + 1, DTag);
      false -> %lager:notice("RabbitMQ confirmed Tag ~p",[DTag]),
               [DTag]
   end,
%%   lager:notice("bunny_worker ~p has pending_acks: ~p~n acks: ~p",[self(), State#state.pending_acks, Tags]),
%%   lager:notice("bunny_worker ~p has pending_acks: ~p",[self(), map_size(State#state.pending_acks)]),
   % @todo: check if use of multiple ack for esq is more efficient
   [esq:ack(Ack, Q) || {_T, Ack} <- maps:to_list(maps:with(Tags, Pending))],

%%   lager:notice("new pending: ~p",[maps:without(Tags, Pending)]),
%%   lager:notice("acked esq: ~p",[maps:to_list(maps:with(Tags, Pending))]),
   {noreply, State#state{last_confirmed_dtag = DTag, pending_acks = maps:without(Tags, Pending)}};

handle_info(#'basic.return'{reply_text = RText, routing_key = RKey}, State) ->
   lager:info("Rabbit returned message: ~p",[{RText, RKey}]),
   {noreply, State};

handle_info(#'basic.nack'{delivery_tag = _DTag, multiple = _Multiple}, State=#state{config = AmqpParams}) ->
   Host = proplists:get_value(host, AmqpParams, <<"unknown">>),
   lager:warning("Rabbit at ~p nacked message: ~p",[Host, {_DTag}]),
   {noreply, State};

handle_info(#'channel.flow'{}, State) ->
   lager:warning("AMQP channel in flow control: ~p",[State#state.channel]),
   {noreply, State};

handle_info(#'channel.flow_ok'{}, State) ->
   lager:info("AMQP channel released flow control: ~p",[State#state.channel]),
   {noreply, State};

handle_info(#'connection.blocked'{}, State) ->
   lager:warning("Rabbit blocked Connection: ~p",[State#state.connection]),
   {noreply, State#state{available = false}};

handle_info(#'connection.unblocked'{}, State = #state{deq_timer_ref = T}) ->
   lager:warning("Rabbit unblocked Connection: ~p",[State#state.connection]),
   catch (erlang:cancel_timer(T)),
   {noreply, maybe_start_deq_timer(State#state{available = true})};

handle_info(report_pendinglist_length, #state{pending_acks = P} = State) ->
   lager:notice("Bunny-Worker PendingList-length: ~p", [{self(), length(P)}]),
   {noreply, State};
handle_info(stop,  State) ->
%%   catch(close(Channel, Conn, State)),
   {stop, normal, State};
handle_info(Msg, State = #state{channel = Chan}) ->
   lager:notice("Bunny-Worker got unexpected msg: ~p, my chan is :~p", [Msg, Chan]),
   {noreply, State}.

handle_call(Req, _From, State) ->
   lager:notice("Invalid request: ~p", [Req]),
   {reply, invalid_request, State}.


-spec terminate(atom(), state()) -> ok.
terminate(Reason, State=#state{channel = Channel, connection = Conn}) ->
   close(Channel, Conn, State),
   lager:notice("~p ~p terminating with reason: ~p",[?MODULE, self(), Reason]).

close(Channel, Conn, #state{queue = _Q, last_confirmed_dtag = _LastTag, pending_acks = _Pending, deq_timer_ref = T}) ->
   catch (erlang:cancel_timer(T)),
   amqp_channel:unregister_confirm_handler(Channel),
   amqp_channel:unregister_return_handler(Channel),
   amqp_channel:unregister_flow_handler(Channel),
   amqp_channel:close(Channel),
   amqp_connection:close(Conn).

-spec code_change(string(), state(), term()) -> {ok, state()}.
code_change(_OldVsn, State, _Extra) ->
   {ok, State}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Private API.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
next(#state{queue = Q, adaptive_interval = AdaptInt} = State) ->
   NewState =
      case esq:deq(Q) of
         [] ->
            {NewIntervalM, NewAdaptIntM} = adaptive_interval:in(miss, AdaptInt),
%%            lager:notice("q miss, new int: ~p", [lager:pr(NewAdaptIntM, adaptive_interval)]),
            State#state{adaptive_interval = NewAdaptIntM, deq_interval = NewIntervalM};
         [#{payload := Payload, receipt := Receipt}] ->
            {NewIntervalH, NewAdaptIntH} = adaptive_interval:in(hit, AdaptInt),
%%            lager:notice("q hit, new int: ~p", [lager:pr(NewAdaptIntH, adaptive_interval)]),
            deliver(Payload, Receipt, State#state{deq_interval = NewIntervalH, adaptive_interval = NewAdaptIntH})
      end,
   maybe_start_deq_timer(NewState).

deliver({_Exchange, _Key, _Payload, _Args}, _QReceipt, State = #state{available = false, mem_q = undefined}) ->
   State;
deliver({_Exchange, _Key, _Payload, _Args} = M, _QReceipt, State = #state{available = false, mem_q = Q}) ->
   NewQ = memory_queue:enq(M, Q),
   State#state{mem_q = NewQ};
deliver({Exchange, Key, Payload, Args}, QReceipt, State = #state{channel = Channel, delivery_mode = DeliveryMode}) ->
   NextSeqNo = amqp_channel:next_publish_seqno(Channel),

   Publish = #'basic.publish'{mandatory = false, exchange = Exchange, routing_key = Key},
   Message = #amqp_msg{payload = Payload,
      props = #'P_basic'{delivery_mode = DeliveryMode, correlation_id = corr_id(Key, Payload), headers = Args}
   },
   NewState =
      case amqp_channel:call(Channel, Publish, Message) of
         ok ->
            case State#state.safe_mode of
               true ->
                  PenList = maps:put(NextSeqNo, QReceipt, State#state.pending_acks),
%%            lager:info("put pending tag: ~p", [NextSeqNo]),
                  State#state{pending_acks = PenList};
               false ->
                  State
            end;
         Error ->
            lager:warning("error when calling channel : ~p", [Error]),
            State
      end,
   NewState.

maybe_redeliver(S = #state{mem_q = Q, queue = undefined}) ->
   {Items, NewQ} = memory_queue:to_list_reset(Q),
   F = fun(Item, StateAcc) ->
      deliver(Item, 0, StateAcc)
      end,
   lists:foldl(F, S#state{mem_q = NewQ}, Items);
maybe_redeliver(S = #state{mem_q = undefined}) ->
   S.

maybe_start_deq_timer(State = #state{queue = undefined}) ->
   State;
maybe_start_deq_timer(State = #state{deq_interval = Interval}) ->
   TRef = erlang:send_after(Interval, self(), deq),
   State#state{deq_timer_ref = TRef}.

corr_id(Key, Payload) ->
   integer_to_binary(erlang:phash2([Key, Payload])).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% MQ Connection functions.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
start_connection(State = #state{config = Config, reconnector = Recon, safe_mode = Safe}) ->
%%   lager:notice("amqp_params: ~p",[lager:pr(Config, ?MODULE)] ),
   Connection = maybe_start_connection(State),
   NewState =
   case Connection of
      {ok, Conn} ->
         Channel = new_channel(Connection, Safe),
         case Channel of
            {ok, Chan} ->
               %% we link to the channel, to get the EXIT signal
               link(Chan),
               NState = State#state{connection = Conn, channel = Chan, available = true,
                  reconnector = faxe_backoff:reset(Recon)},
               NState;
            Er ->
               lager:warning("Error starting channel: ~p",[Er]),
               {ok, Reconnector} = faxe_backoff:execute(Recon, connect),
               State#state{available = false, reconnector = Reconnector}
         end;
      E ->
         lager:warning("Error starting amqp connection: ~p :: ~p",[Config, E]),
         {ok, Reconnector} = faxe_backoff:execute(Recon, connect),
         State#state{available = false, reconnector = Reconnector}
   end,
   NewState.

new_channel({ok, Connection}, SafeMode) ->
   amqp_connection:register_blocked_handler(Connection, self()),
   %% using a random channel number here, because we saw issues with using the same number
   %% after a crash where it seems like the server did not release the number yet and reports a
   %% CHANNEL_ERROR - second 'channel.open' seen (this is the case, when a channel-number is used twice)
   ChannelNumber = rand:uniform(?MAX_CHANNEL_NUMBER),
   configure_channel(amqp_connection:open_channel(Connection, ChannelNumber), SafeMode);

new_channel(Error, _) ->
   lager:warning("Error connecting to broker: ~p",[Error]),
   Error.

configure_channel({ok, Channel}, false) ->
   preconfig_channel(Channel),
   {ok, Channel};
configure_channel({ok, Channel}, true) ->
   preconfig_channel(Channel),

   case amqp_channel:call(Channel, #'confirm.select'{}) of
      {'confirm.select_ok'} ->
         {ok, Channel};
      Error ->
         lager:error("Could not configure channel: ~p", [Error]),
         Error
   end;

configure_channel(Error, _) ->
   Error.

preconfig_channel(Channel) ->
   ok = amqp_channel:register_flow_handler(Channel, self()),
   ok = amqp_channel:register_confirm_handler(Channel, self()),
   ok = amqp_channel:register_return_handler(Channel, self()).

maybe_start_connection(#state{connection = Conn, config = Config}) ->
   case is_pid(Conn) andalso is_process_alive(Conn) of
      true ->
         {ok, Conn};
      false ->
         case amqp_connection:start(Config) of
            {ok, NewConn} = Res -> erlang:monitor(process, NewConn), Res;
            Other -> Other
         end
   end.