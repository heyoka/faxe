%% Copyright
-module(influx_worker).
-author("Alexander Minichmair").

-behaviour(gen_server).

-include("../include/rmcfirehose.hrl").

%% API
-export([start_link/2]).

%% gen_server callbacks
-export([init/1,
  handle_call/3,
  handle_cast/2,
  handle_info/2,
  terminate/2,
  code_change/3]).


-record(state, {
               items = []        :: list(),
               config            :: term(),
               flush_count       :: non_neg_integer(),
               flush_timeout     :: non_neg_integer(),
               timer             :: undefined,
               item_count = 0    :: non_neg_integer(),
               last_tag          :: non_neg_integer(),
               parent            :: pid(),
               tags_fields = []  :: list()

         }
).

-define(DB,             <<"lm3">>).
-define(MEASUREMENT,    <<"m1">>).

-define(VALID_FIELDS, [ <<"val">>, <<"min">>, <<"max">>, <<"val_original">>,
                        <<"min_original">>, <<"max_original">>, <<"cnt">>,
                        <<"lat">>, <<"long">>, <<"num">>]).

-define(VALID_ADD_FIELDS, [
   <<"val_original">>, <<"min_original">>, <<"max_original">>
]).


-define(ERROR_RETRIES, 3).

-define(FAILED_RETRIES, 7).

-define(NOT_AVAIL_TIMEOUT, 3000).

-define(TIME_PRECISION, <<"ms">>).

-define(SEND_FAILED, influx_worker_failed).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
-spec(start_link(FlushCount :: non_neg_integer(), FlushTimeout :: non_neg_integer()) ->
  {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link(FlushCount, FlushTimeout) ->
  gen_server:start_link(?MODULE, [FlushCount, FlushTimeout, self()], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
-spec(init(Args :: term()) ->
  {ok, State :: #state{}} | {ok, State :: #state{}, timeout() | hibernate} |
  {stop, Reason :: term()} | ignore).
init([FlushCount, FlushTimeout, ParentPid]) ->
  {ok, #state{flush_count = FlushCount, flush_timeout = FlushTimeout, parent = ParentPid}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @end
%%--------------------------------------------------------------------
-spec(handle_call(Request :: term(), From :: {pid(), Tag :: term()},
    State :: #state{}) ->
  {reply, Reply :: term(), NewState :: #state{}} |
  {reply, Reply :: term(), NewState :: #state{}, timeout() | hibernate} |
  {noreply, NewState :: #state{}} |
  {noreply, NewState :: #state{}, timeout() | hibernate} |
  {stop, Reason :: term(), Reply :: term(), NewState :: #state{}} |
  {stop, Reason :: term(), NewState :: #state{}}).

handle_call(flush, _From, State) ->
   {reply, ok, flush_if_any(State)}
;
handle_call(_Request, _From, State) ->
  lager:warning("Undefined Call Request in influx_worker gen_server: ~p from ~p",[_Request, _From]),
  {reply, ok, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @end
%%--------------------------------------------------------------------
-spec(handle_cast(Request :: term(), State :: #state{}) ->
  {noreply, NewState :: #state{}} |
  {noreply, NewState :: #state{}, timeout() | hibernate} |
  {stop, Reason :: term(), NewState :: #state{}}).
handle_cast({publish, {Message, StreamId, Tag}},
    State = #state{ items = [], flush_timeout = FlushTimeout, timer = undefined}) ->

   % this is the first item to be queued
   % so we start the flush timer
   Timer = erlang:send_after(FlushTimeout, self(), flush),
   handle_cast({publish, {Message, StreamId, Tag}}, State#state{ timer = Timer })
;
handle_cast({publish, {Message, StreamId, Tag}}, State=#state{tags_fields = _TF}) ->
   Ty = proplists:get_value(<<"ty">>, Message),

   {NTags, NFields} = tags_fields(Message, StreamId, Ty),
   Ts = proplists:get_value(<<"ts">>, Message, util:current_ms()),
   NewQueue = [{?MEASUREMENT, NTags, NFields, Ts} | State#state.items],

   NewState = State#state{items = NewQueue, item_count = State#state.item_count + 1, last_tag = Tag},
   {noreply, maybe_flush(NewState)}
;
handle_cast(_Req, State) ->
  lager:warning("Unexpected cast message in ~p : ~p ",[?MODULE, _Req]),
  {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
-spec(handle_info(Info :: timeout() | term(), State :: #state{}) ->
  {noreply, NewState :: #state{}} |
  {noreply, NewState :: #state{}, timeout() | hibernate} |
  {stop, Reason :: term(), NewState :: #state{}}).
%%
handle_info(flush, State) ->
   {noreply, flush_if_any(State)}
;
handle_info(_Info, State) ->
  lager:debug("influx_worker got unexpected Info : ~p",[_Info]),
  {noreply, State}.

%%--------------------------------------------------------------------
-spec(terminate(Reason :: (normal | shutdown | {shutdown, term()} | term()),
    State :: #state{}) -> term()).
terminate(_Reason, #state{}=_State) ->
  ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
-spec(code_change(OldVsn :: term() | {down, term()}, State :: #state{},
    Extra :: term()) ->
  {ok, NewState :: #state{}} | {error, Reason :: term()}).
code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================


-spec maybe_flush(#state{}) -> #state{}.
maybe_flush(State = #state{ flush_count = SameCount, item_count = SameCount }) ->
   flush_if_any(State);
maybe_flush(State) ->
   State.


-spec flush_if_any(#state{}) -> #state{}.
flush_if_any(State = #state{ items = [] }) ->
   cancel_timer(State);
flush_if_any(State = #state{}) ->
   flush(State),
   cancel_timer(State#state{ items = [], item_count = 0, last_tag = undefined}).


flush(State = #state{items = Items}) when is_list(Items) ->
   try_send(State, ?ERROR_RETRIES, ?FAILED_RETRIES).

try_send(#state{items = _Items, last_tag = Tag, parent = Parent}, 0, _) ->
   lager:warning("~p Could not write batch with ~p send-retries: ~n~p", [?MODULE, ?ERROR_RETRIES, _Items]),
   rate ! {bump, length(_Items), ?SEND_FAILED},
   carrot:ack_multiple(Parent, Tag)
;
try_send(#state{items = _Items, last_tag = Tag, parent = Parent}, _, 0) ->
   lager:warning("~p Could not write batch with ~p failed-retries: ~n~p", [?MODULE, ?FAILED_RETRIES, _Items]),
%%   carrot:reject(Parent, Tag),
   rate ! {bump, length(_Items), ?SEND_FAILED},
   carrot:ack_multiple(Parent, Tag)
;
try_send(State=#state{items = Items, last_tag = Tag, parent = Parent}, Retries, FailedRetries) ->
   case catch(fluxer:write_batch(?DB, Items, ?TIME_PRECISION)) of
      ok                -> %% acknowledge multiple until our last Tag
                           rate ! {bump, length(Items), ?MODULE},
                           carrot:ack_multiple(Parent, Tag);

      Other             -> lager:warning("Problem when sending influx batch: ~p" ,[Other]),

                           case eval_response(Other) of
                              not_avaiable   ->
                                 lager:error("Influx-Http-Endpoint not available ! ... will sleep ~p sec ...", [?NOT_AVAIL_TIMEOUT/1000]),
                                 timer:sleep(?NOT_AVAIL_TIMEOUT),
                                 try_send(State, Retries, FailedRetries);

                              failed         ->
                                 timer:sleep((?FAILED_RETRIES-FailedRetries+1) * 1000),
                                 try_send(State, Retries, FailedRetries -1);

                              {error, emfile}    ->
                                 lager:alert("Filedescriptors exhausted ! ... will sleep ~p sec ...", [?NOT_AVAIL_TIMEOUT/1000]),
                                 timer:sleep(?NOT_AVAIL_TIMEOUT),
                                 try_send(State, Retries, FailedRetries);

                              {error, _W}    ->
                                 timer:sleep(Retries*300),
                                 try_send(State, Retries -1, FailedRetries)
                           end
   end.


-spec cancel_timer(#state{}) -> #state{}.
cancel_timer(State = #state{ timer = undefined }) ->
   State;
cancel_timer(State = #state{ timer = Timer }) ->
   erlang:cancel_timer(Timer),
   State#state{ timer = undefined }.


-spec tags_fields(list(), binary(), binary()) -> {list(), list()}.
tags_fields(MessageList, RKey, Ty) ->
   {VFields, VTags} =
   case ets:lookup(?ETS_STREAM_TYPES, Ty) of
      []                      -> {?VALID_FIELDS, []};
      [{Ty, {Fields0, Tags0}}]  -> {Fields0 ++ ?VALID_ADD_FIELDS, Tags0}
   end,
   Fields = filter_list(MessageList, VFields),
   Tags = filter_list(MessageList, VTags),
%%   lager:info("Fields: ~p ~n Tags: ~p",[Fields, [{<<"stream">>, RKey} | Tags]]),
   add_keytag(RKey, Tags, Fields).


-spec add_keytag(binary(), list(), list()) -> tuple().
add_keytag(RKey, Tags, Fields) ->
   {[{<<"stream">>, RKey} | Tags], Fields}.

filter_list([], _ValidEntries) ->
   [];
filter_list(List, ValidEntries) ->
   lists:filter(fun({Key, Val}) -> lists:member(Key, ValidEntries) andalso Val =/= null end, List).

%%
%% @doc
%% evaluate the fluxer response and decide on error-state
%%
%% the function returns 'failed', if the error ocurred, because there has been something wrong with
%% transport, influxdb itself had an error or an upstream process died
%%
%% it will return {error, Msg} if any other error (such as a Bad Request) occurs
%%
%% 'not_available' is returned, if the service could not be reached
%%
%% now, on any unknown Response, the function will also return 'failed' !
%%
%% @end
%%
-spec eval_response(tuple()|any()) -> 'failed' | {'error', any()}.
eval_response({ok, {{<<"200">>, _}, _Hdrs, _Resp, _, _}})            -> failed;
eval_response({ok, {{<<"4", _R:2/binary>>, _}, _Hdrs, _Resp, _, _}}) -> {error, _Resp};
eval_response({ok, {{<<"503">>, _}, _Hdrs, _Resp, _, _}})            -> not_avaiable;
eval_response({ok, {{<<"5", _R:2/binary>>, _}, _Hdrs, _Resp, _, _}}) -> failed;
eval_response({'EXIT',_})                                            -> failed;
eval_response(_What)                                                 -> failed.
