%%%-------------------------------------------------------------------
%%% @author heyoka
%%% @copyright (C) 2020
%%% @doc
%%% @end
%%%-------------------------------------------------------------------
-module(q_msg_forwarder).

-behaviour(gen_server).

-export([start_link/1, start_monitor/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
   code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {
   queue,
   parent,
   subscriptions,
   deq_interval = 15
}).

%%%===================================================================
%%% Spawning and gen_server implementation
%%%===================================================================

start_link(Queue) ->
   gen_server:start_link(?MODULE, [Queue, self()], []).

start_monitor(Queue) ->
   {ok, Pid} = gen_server:start(?MODULE, [Queue, self()], []),
   _ = erlang:monitor(process, Pid),
   {ok, Pid}.

init([{_,_}=Idx, Parent]) ->
   QFile = faxe_config:q_file(Idx),
   {ok, Q} = esq:new(QFile, faxe_config:get_esq_opts()),
   init([Q, Parent]);
init([Queue, Parent]) ->
   State = #state{queue = Queue, parent = Parent},
   erlang:send_after(0, self(), setup),
   {ok, State}.

handle_call(_Request, _From, State = #state{}) ->
   {reply, ok, State}.

handle_cast(_Request, State = #state{}) ->
   {noreply, State}.

handle_info(deq, State = #state{}) ->
   next(State),
   {noreply, State};
handle_info(setup, State = #state{parent = Parent}) ->
   {ok, Subs} = gen_server:call(Parent, get_subscribers),
   NewState = State#state{subscriptions = Subs},
   next(NewState),
   {noreply, NewState}.

terminate(_Reason, _State = #state{}) ->
   ok.

code_change(_OldVsn, State = #state{}, _Extra) ->
   {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
next(State=#state{queue = Q, deq_interval = Interval}) ->
   case esq:deq(Q) of
      [] -> ok; %lager:info("Queue is empty!"), ok;
      [#{payload := M}] ->
%%         lager:notice("~p: msg from Q: ~p", [faxe_time:now(), M]),
         publish(M, State)
   end,
   erlang:send_after(Interval, self(), deq).

publish(Message, #state{subscriptions = Subs}) ->
   df_subscription:output(Subs, Message, 1).