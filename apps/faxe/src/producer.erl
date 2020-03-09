%%%-------------------------------------------------------------------
%%% @author heyoka
%%% @copyright (C) 2020, <COMPANY>
%%% @doc
%%% @end
%%%-------------------------------------------------------------------
-module(producer).

-behaviour(gen_server).

-export([start_link/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
   code_change/3]).

-define(SERVER, ?MODULE).
-define(DEFAULT_QUEUE_FILE, "/tmp/test_q").
-define(Q_TTS, 500).
-define(INTERVAL, 50).

-record(producer_state, {
   q
}).

%%%===================================================================
%%% Spawning and gen_server implementation
%%%===================================================================

start_link() ->
   gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

init([]) ->
   {ok, Q} = esq:new(?DEFAULT_QUEUE_FILE, [{tts, ?Q_TTS}, {capacity, 5}, {ttf, 5000}]),
   consumer:start_link(Q),
   produce(Q),
   {ok, #producer_state{q = Q}}.

handle_call(_Request, _From, State = #producer_state{}) ->
   {reply, ok, State}.

handle_cast(_Request, State = #producer_state{}) ->
   {noreply, State}.

handle_info(enq, State = #producer_state{q = Q}) ->
   produce(Q),
   {noreply, State}.

terminate(_Reason, _State = #producer_state{}) ->
   ok.

code_change(_OldVsn, State = #producer_state{}, _Extra) ->
   {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
produce(Q) ->
   V = faxe_lambda_lib:random_real(200),
   esq:enq({faxe_time:now(), V}, Q),
   erlang:send_after(?INTERVAL, self(), enq).