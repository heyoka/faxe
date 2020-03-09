%%%-------------------------------------------------------------------
%%% @author heyoka
%%% @copyright (C) 2020, <COMPANY>
%%% @doc
%%% @end
%%%-------------------------------------------------------------------
-module(q_msg_forwarder).

-behaviour(gen_server).

-export([start_link/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
   code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {
   queue,
   subscriptions,
   deq_interval = 15
}).

%%%===================================================================
%%% Spawning and gen_server implementation
%%%===================================================================

start_link(Queue) ->
   gen_server:start_link({local, ?SERVER}, ?MODULE, [Queue, self()], []).

init([Queue, Parent]) ->
   {ok, Subs} = gen_server:call(Parent, get_subscribers),
   State = #state{queue = Queue, subscriptions = Subs},
   next(State),
   {ok, State}.

handle_call(_Request, _From, State = #state{}) ->
   {reply, ok, State}.

handle_cast(_Request, State = #state{}) ->
   {noreply, State}.

handle_info(_Info, State = #state{}) ->
   {noreply, State}.

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
         lager:notice("~p: msg from Q: ~p", [faxe_time:now(), M]),
         publish(M, State)
   end,
   erlang:send_after(Interval, self(), deq).

publish(Message, #state{subscriptions = Subs}) ->
   df_subscription:output(Subs, Message, 1).