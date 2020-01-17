%%% @doc MQ publishing-worker.
%%% @author Alexander Minichmair @ LineMetrics GmbH
%%%
-module(bunny_worker).


-behaviour(gen_server).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Types.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Required Types.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-include_lib("amqp_client/include/amqp_client.hrl").

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Types.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-record(state, {
   connection           = undefined :: undefined|pid(),
   channel              = undefined :: undefined|pid(),
   channel_ref          = undefined :: undefined|reference(),
   config               = []        :: proplists:proplist(),
   available            = false     :: boolean(),
   pending_acks         = []        :: list(),
   last_confirmed_dtag  = 0         :: non_neg_integer()
}).

-type state():: #state{}.

-define(DELIVERY_MODE, 1).

-define(TRY_MAX_WORKERS, 7).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Exports.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Public API.
-export([deliver/2, start_link/1, stop/1, start/1]).

%%% gen_server/worker_pool callbacks.
-export([
   init/1, terminate/2, code_change/3,
   handle_call/3, handle_cast/2, handle_info/2
]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Public API.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec start_link(any()) -> any().
start_link(Args) ->
   gen_server:start_link(?MODULE, Args, []).

start(Args) ->
   gen_server:start(?MODULE, Args, []).

stop(Server) ->
   Server ! stop.

-spec deliver(pid(), tuple()) -> {ok, reference()}|{error, term()}.
deliver(Server, {Exchange, Key, Payload, Args}) ->
   gen_server:call(Server, {deliver, {Exchange, Key, Payload, Args, self()}}).

%% @doc confirmation required from the mq.
%%-spec deliver(binary(), binary(), binary(), list()) -> ok|term().
%%deliver(Exchange, Key, Payload, Args) ->
%%   ensure_deliver(Exchange, Key, Payload, Args, ?TRY_MAX_WORKERS).
%%
%%ensure_deliver(Exchange, Key, Payload, Args, 0) ->
%%   wpool:call(?MODULE, {deliver, {Exchange, Key, Payload, Args, self()}}, next_worker);
%%ensure_deliver(Exchange, Key, Payload, Args, Tries) ->
%%   case wpool:call(?MODULE, {deliver, {Exchange, Key, Payload, Args, self()}}, next_worker) of
%%      not_available ->  lager:notice("bunny-worker not available, try next worker ..."),
%%                        ensure_deliver(Exchange, Key, Payload, Args, Tries-1);
%%      Other -> Other
%%   end.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% gen_server API.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec init(proplists:proplist()) -> {ok, state()}.
init(Config) ->
   process_flag(trap_exit, true),
   lager:debug("bunny_worker is starting"),
   erlang:send_after(0, self(), connect),
   {ok, #state{config = Config}}.

-spec handle_cast(term(), state()) -> {noreply, state()}.
handle_cast(Msg, State) ->
   lager:error("Invalid cast: ~p in ~p", [Msg, ?MODULE]),
   {noreply, State}.

%%%%%%%%%%%%%%%%%%%

-spec handle_info(term(), state()) -> {noreply, state()}.
handle_info(connect, State) ->
   {Available, Channel, Conn} = check_for_channel(State),
   {noreply, State#state{
      channel = Channel,
      available = Available,
      connection = Conn
   }};

handle_info({'EXIT', MQPid, Reason}, State=#state{channel = MQPid} ) ->
   lager:warning("MQ channel DIED: ~p", [Reason]),
   erlang:send_after(0, self(), connect),
   {noreply, State#state{
      channel = undefined,
      channel_ref = undefined,
      available = false
   }};


%% @doc
%% We handle the ack, nack, etc... - messages with these functions
%% the last delivery tag will be stored in #state
%% if the 'multiple' flag is set, the sequence is : |- from ('#state.last_confirmed_dtag' + 1) to DTag -|
%%
%% this function will release the given leases (delivery_tag(s)) in sending the Tag to the
%% requester of the corresponding publish-call
%% @end
handle_info(#'basic.ack'{delivery_tag = DTag, multiple = Multiple}, State) ->
   Tags =
   case Multiple of
      true -> lager:warning("RabbitMQ confirmed MULTIPLE Tags till ~p",[DTag]),
               lists:seq(State#state.last_confirmed_dtag + 1, DTag);
      false -> lager:notice("RabbitMQ confirmed Tag ~p",[DTag]),[DTag]
   end,
%%   lager:notice("bunny_worker ~p has pending_acks: ~p",[self(), State#state.pending_acks]),
   NewPendingList = handle_ack(Tags, State#state.pending_acks),
   {noreply, State#state{last_confirmed_dtag = DTag, pending_acks = NewPendingList}};

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
   lager:notice("Bunny-Worker got stop msg"),
   {stop, normal, State};
handle_info(Msg, State) ->
   lager:notice("Bunny-Worker got unexpected msg: ~p", [Msg]),
   {noreply, State}.


-spec handle_call(
    term(), {pid(), reference()}, state()
) -> {reply, term() | {invalid_request, term()}, state()}.

handle_call({deliver, _}, _From, State=#state{available = false}) ->
   {reply, not_available, State}
;
handle_call({deliver, {Exchange, Key, Payload, Args, Requester}}, _From, State=#state{channel = Channel}) ->
   NextSeqNo = amqp_channel:next_publish_seqno(Channel),
   Ref = make_ref(),
   Publish = #'basic.publish'{mandatory = false, exchange = Exchange, routing_key = Key},
   Message = #amqp_msg{payload = Payload,
                        props = #'P_basic'{delivery_mode = ?DELIVERY_MODE, headers = Args}
   },
   {Ret, NewState} =
      case amqp_channel:call(Channel, Publish, Message) of
            ok ->
               PenList = [{NextSeqNo, {Requester, Ref}} | State#state.pending_acks],
               {{ok ,Ref}, State#state{pending_acks = PenList}};
            Error -> {Error, State}
         end,

   {reply, Ret, NewState};

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
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% MQ Connection functions.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
do_connect(State) ->
   {Available, Channel, Conn} = check_for_channel(State),
   {noreply, State#state{
      channel = Channel,
      available = Available,
      connection = Conn
   }}.

check_for_channel(#state{} = State) ->
   Connect = fun() ->
      case connect(State#state.config) of
         {{ok, Pid},{ok,Conn}} -> {Pid,Conn};
         Error -> lager:warning("MQ NOT available: ~p", [Error]), not_available
      end
             end,
   {Channel, Conn} =
      case State#state.channel of
         {Pid,Conn0} when is_pid(Pid) -> case is_process_alive(Pid) of
                                            true -> {Pid, Conn0};
                                            false -> Connect()
                                         end;
         _ -> Connect()
      end,
   Available = is_pid(Channel),
   {Available, Channel, Conn}.

connect(Config) ->
   Get = fun
      ({s, X}) ->
         case proplists:get_value(X, Config) of
            Val when is_list(Val) -> list_to_binary(Val);
            Bin -> Bin
         end;
      (X) ->
         proplists:get_value(X, Config) end,
   GetWithDefault = fun(X, Default) ->
      case Get(X) of
         undefined -> Default;
         Value -> Value
      end
   end,
   RabbbitHosts = Get(hosts),
   rand:seed(exs1024s),
   Index = rand:uniform(length(RabbbitHosts)),
   {Host, Port} = lists:nth(Index,RabbbitHosts),
   Connection = amqp_connection:start(#amqp_params_network{
      username = Get({s, user}),
      password = Get({s, pass}),
      virtual_host = Get({s, vhost}),
      port = Port,
      host = Host,
      heartbeat = GetWithDefault(heartbeat, 80),
      ssl_options = GetWithDefault(ssl_options, none)
   }),
   {new_channel(Connection), Connection}.

new_channel({ok, Connection}) ->
   amqp_connection:register_blocked_handler(Connection, self()),
   configure_channel(amqp_connection:open_channel(Connection));

new_channel(Error) ->
   Error.

configure_channel({ok, Channel}) ->
   ok = amqp_channel:register_flow_handler(Channel, self()),
   ok = amqp_channel:register_confirm_handler(Channel, self()),
   ok = amqp_channel:register_return_handler(Channel, self()),

   case amqp_channel:call(Channel, #'confirm.select'{}) of
      {'confirm.select_ok'} -> {ok, Channel};
      Error -> lager:error("Could not configure channel: ~p", [Error]), Error
   end;

configure_channel(Error) ->
   Error.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc
%%
%% handle a list of acknowledged RMQ-Tags
%% informs interested processes about the acks
%% and removes the tags from the list
%%
%% @end

-spec handle_ack(list(), list()) -> list().
handle_ack([], PendingList) ->
   PendingList;
handle_ack([Tag|Rest], PendingList) when is_list(PendingList)->
   requester_ack(Tag, PendingList),
   handle_ack(Rest, proplists:delete(Tag, PendingList)).


requester_ack(DTag, Pending) ->

   case proplists:get_value(DTag, Pending) of
      undefined   -> false;
      {Pid, Ref}  ->  Pid ! {publisher_ack, Ref}
%%                        ets:delete(?DTAGS_TABLE, DTag)
   end.