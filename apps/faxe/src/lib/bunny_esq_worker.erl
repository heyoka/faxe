%%% @doc MQ publishing-worker.
%%% @author Alexander Minichmair
%%%
-module(bunny_esq_worker).
-behaviour(gen_server).
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Required Types.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-include_lib("amqp_client/include/amqp_client.hrl").

-record(state, {
   connection           = undefined :: undefined|pid(),
   channel              = undefined :: undefined|pid(),
   channel_ref          = undefined :: undefined|reference(),
   config               = []        :: proplists:proplist(),
   available            = false     :: boolean(),
   pending_acks         = #{}       :: map(),
   last_confirmed_dtag  = 0         :: non_neg_integer(),
   queue,
   deq_timer_ref
}).

-type state():: #state{}.

-define(DELIVERY_MODE, 1).
-define(DEQ_INTERVAL, 15).

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
   Server ! stop.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% gen_server API.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec init(list()) -> {ok, state()}.
init([Queue, Config]) ->
   process_flag(trap_exit, true),
   lager:info("bunny_worker is starting"),
   erlang:send_after(0, self(), connect),
   {ok, #state{queue = Queue, config = amqp_options:parse(Config)}}.

-spec handle_cast(term(), state()) -> {noreply, state()}.
handle_cast(Msg, State) ->
   lager:error("Invalid cast: ~p in ~p", [Msg, ?MODULE]),
   {noreply, State}.

%%%%%%%%%%%%%%%%%%%

-spec handle_info(term(), state()) -> {noreply, state()}.
handle_info(connect, State) ->
   NewState = start_connection(State),
   case NewState#state.available of
      true -> {noreply, start_deq_timer(NewState)};
      false -> {noreply, NewState}
   end;

handle_info({'EXIT', MQPid, Reason}, State=#state{channel = MQPid} ) ->
   lager:warning("MQ channel DIED: ~p", [Reason]),
   erlang:send_after(0, self(), connect),
   {noreply, State#state{
      channel = undefined,
      channel_ref = undefined,
      available = false
   }};

%% dequeue
handle_info(deq, State = #state{available = false}) ->
   {noreply, State};
handle_info(deq, State = #state{available = true}) ->
   {noreply, next(State)};


%% @doc
%% We handle the ack, nack, etc... - messages with these functions
%% the last delivery tag will be stored in #state
%% if the 'multiple' flag is set, the sequence is : |- from ('#state.last_confirmed_dtag' + 1) to DTag -|
%%
%% this function will release the given leases (delivery_tag(s)) in acking the stored esq-receipts
%% @end
handle_info(#'basic.ack'{delivery_tag = DTag, multiple = Multiple},
    State = #state{queue = Q, pending_acks = Pending}) ->
   Tags =
   case Multiple of
      true -> lager:warning("RabbitMQ confirmed MULTIPLE Tags till ~p",[DTag]),
               lists:seq(State#state.last_confirmed_dtag + 1, DTag);
      false -> lager:notice("RabbitMQ confirmed Tag ~p",[DTag]),[DTag]
   end,
%%   lager:notice("bunny_worker ~p has pending_acks: ~p~n acks: ~p",[self(), State#state.pending_acks, Tags]),
   [esq:ack(Ack, Q) || {_T, Ack} <- maps:to_list(maps:with(Tags, Pending))],
   lager:notice("new pending: ~p",[maps:without(Tags, Pending)]),
   {noreply, State#state{last_confirmed_dtag = DTag, pending_acks = maps:without(Tags, Pending)}};

handle_info(#'basic.return'{reply_text = RText, routing_key = RKey}, State) ->
   lager:info("Rabbit returned message: ~p",[{RText, RKey}]),
   {noreply, State};

handle_info(#'basic.nack'{delivery_tag = _DTag, multiple = _Multiple}, State) ->
   lager:warning("Rabbit nacked message: ~p",[{_DTag}]),
   {noreply, State};

handle_info(#'channel.flow'{}, State) ->
   lager:warning("Rabbit blocked channel: ~p",[State#state.channel]),
   {noreply, State};

handle_info(#'channel.flow_ok'{}, State) ->
   lager:info("Rabbit unblocked channel: ~p",[State#state.channel]),
   {noreply, State};

handle_info(#'connection.blocked'{}, State) ->
   lager:warning("Rabbit blocked Connection: ~p",[State#state.connection]),
   {noreply, State#state{available = false}};

handle_info(#'connection.unblocked'{}, State) ->
   lager:warning("Rabbit unblocked Connection: ~p",[State#state.connection]),
   {noreply, State#state{available = true}};

handle_info(report_pendinglist_length, #state{pending_acks = P} = State) ->
   lager:notice("Bunny-Worker PendingList-length: ~p", [{self(), length(P)}]),
   {noreply, State};
handle_info(stop, State) ->
   {stop, normal, State};
handle_info(Msg, State) ->
   lager:notice("Bunny-Worker got unexpected msg: ~p", [Msg]),
   {noreply, State}.

handle_call(Req, _From, State) ->
   lager:notice("Invalid request: ~p", [Req]),
   {reply, invalid_request, State}.


-spec terminate(atom(), state()) -> ok.
terminate(Reason, #state{channel = Channel, connection = Conn}) ->
   lager:notice("~p ~p terminating with reason: ~p",[?MODULE, self(), Reason]),
   catch(close(Channel, Conn))
   .

close(Channel, Conn) ->
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
next(#state{queue = Q} = State) ->
   NewState =
      case esq:deq(Q) of
         [] ->
%%            lager:info("esq:deq miss"),
            State;
         [#{payload := Payload, receipt := Receipt}] ->
%%            lager:info("esq:deq hit !"),
            deliver(Payload, Receipt, State)
      end,
   start_deq_timer(NewState).

deliver({_Exchange, _Key, _Payload, _Args}, _QReceipt, State = #state{available = false}) ->
   lager:warning("channel is not availbale"),
   State;
deliver({Exchange, Key, Payload, Args}, QReceipt, State = #state{channel = Channel}) ->
   lager:notice("Channel is: ~p",[Channel]),
   NextSeqNo = amqp_channel:next_publish_seqno(Channel),

   Publish = #'basic.publish'{mandatory = false, exchange = Exchange, routing_key = Key},
   Message = #amqp_msg{payload = Payload,
      props = #'P_basic'{delivery_mode = ?DELIVERY_MODE, headers = Args}
   },
   NewState =
      case amqp_channel:call(Channel, Publish, Message) of
         ok ->
            PenList = maps:put(NextSeqNo, QReceipt, State#state.pending_acks),
            State#state{pending_acks = PenList};
         Error ->
            lager:error("error when calling channel : ~p", [Error]),
            State
      end,
   NewState.

start_deq_timer(State = #state{}) ->
   TRef = erlang:send_after(?DEQ_INTERVAL, self(), deq),
   State#state{deq_timer_ref = TRef}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% MQ Connection functions.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
start_connection(State = #state{config = Config}) ->
   lager:notice("amqp_params: ~p",[lager:pr(Config, ?MODULE)] ),
   Connection = amqp_connection:start(Config),
   NewState =
   case Connection of
      {ok, Conn} ->
         Channel = new_channel(Connection),
         case Channel of
            {ok, Chan} ->
               State#state{connection = Conn, channel = Chan, available = true};
            Er ->
               lager:warning("Error starting channel: ~p",[Er]),
               erlang:send_after(100, self(), connect),
               State#state{available = false}
         end;
      E ->
         lager:warning("Error starting connection: ~p",[E]),
         erlang:send_after(100, self(), connect),
         State#state{available = false}
   end,
   NewState.

new_channel({ok, Connection}) ->
   amqp_connection:register_blocked_handler(Connection, self()),
   configure_channel(amqp_connection:open_channel(Connection));

new_channel(Error) ->
   lager:warning("Error connecting to broker: ~p",[Error]),
   Error.

configure_channel({ok, Channel}) ->
   ok = amqp_channel:register_flow_handler(Channel, self()),
   ok = amqp_channel:register_confirm_handler(Channel, self()),
   ok = amqp_channel:register_return_handler(Channel, self()),

   case amqp_channel:call(Channel, #'confirm.select'{}) of
      {'confirm.select_ok'} -> lager:notice("amqp channel is ok: ~p",[Channel]),{ok, Channel};
      Error -> lager:error("Could not configure channel: ~p", [Error]), Error
   end;

configure_channel(Error) ->
   Error.