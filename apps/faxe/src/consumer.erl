%%%-------------------------------------------------------------------
%%% @author heyoka
%%% @copyright (C) 2020, <COMPANY>
%%% @doc
%%% @end
%%%-------------------------------------------------------------------
-module(consumer).

-behaviour(gen_server).

-export([start_link/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
   code_change/3]).

-define(SERVER, ?MODULE).
-define(INTERVAL, 20).
-define(ACK_RECPS, 9).
-record(consumer_state, {
   q
   ,stop,
   receipts = []
}).

%%%===================================================================
%%% Spawning and gen_server implementation
%%%===================================================================

start_link(Queue) ->
   gen_server:start_link({local, ?SERVER}, ?MODULE, [Queue], []).

init([Q]) ->
   lager:notice("startup at: ~p", [faxe_time:to_iso8601(faxe_time:now())]),
   erlang:send_after(?INTERVAL, self(), deq),
%%   erlang:send_after(1000 * 5, self(), stop_consuming),
   {ok, #consumer_state{q = Q}}.

handle_call(_Request, _From, State = #consumer_state{}) ->
   {reply, ok, State}.

handle_cast(_Request, State = #consumer_state{}) ->
   {noreply, State}.

handle_info(deq, State = #consumer_state{stop = true}) ->
   {noreply, State};
handle_info(deq, State = #consumer_state{q = Q, receipts = Recps}) ->
   NewRecps =
   case esq:deq(Q) of
      [] ->
%%         lager:info("Queue miss!"),
         Recps;
      [#{payload := Payload, receipt := Receipt}] ->
         lager:notice("msg from Q: ~p at: ~p", [Payload, faxe_time:to_iso8601(faxe_time:now())]),
         [Receipt | Recps]
   end,
   Receipts =
   case length(NewRecps) >= ?ACK_RECPS of
      true -> %% acc_all
         lager:info("acc receipts now: ~p", [NewRecps]),
         [esq:ack(R, Q) || R <- NewRecps],
         [];
      false -> NewRecps
   end,
   erlang:send_after(?INTERVAL, self(), deq),
   {noreply, State#consumer_state{receipts = Receipts}};
handle_info(stop_consuming, State) ->
   {noreply, State#consumer_state{stop = true}}.

terminate(_Reason, _State = #consumer_state{}) ->
   ok.

code_change(_OldVsn, State = #consumer_state{}, _Extra) ->
   {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
