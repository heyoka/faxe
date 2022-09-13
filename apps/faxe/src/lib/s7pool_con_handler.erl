%%%-------------------------------------------------------------------
%%% @author heyoka
%%% @copyright (C) 2020, <COMPANY>
%%% @doc
%%% @end
%%%-------------------------------------------------------------------
-module(s7pool_con_handler).

-behaviour(gen_server).

-export([start_link/0, get_connection/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
  code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {
  pool_indices = #{}
}).

%%%===================================================================
%%% Spawning and gen_server implementation
%%%===================================================================
get_connection(Key) ->
  Index = get_index(Key),
  get_connection(Key, Index).
%%  gen_server:call(?SERVER, {get_connection, Key}).

start_link() ->
  gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

init([]) ->
  {ok, #state{}}.

handle_call({get_connection, Key}, _From, State = #state{pool_indices = Idxs}) ->
  Index =
  case maps:is_key(Key, Idxs) of
    true -> maps:get(Key, Idxs);
    false -> 1
  end,
  {Worker, NewIndex} =
  case get_connection(Key, Index) of
    {ok, W, Ind} -> {{ok, W}, Ind};
    {error, _What} = R -> {R, Index}
  end,
%%  lager:info("got worker: ~p at index: ~p",[Worker, NewIndex]),
  {reply, Worker, State#state{pool_indices = #{Key => NewIndex}}};
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
get_connection(Ip, Index) ->
  Index = get_index(Ip),
  case ets:lookup(s7_pools, Ip) of
    [] -> {error, no_pool_found};
    [{Ip, []}] -> {error, no_connection_in_pool};
    [{Ip, [Conn]}] -> {ok, Conn, 1};
    [{Ip, Connections}] ->
      NextI = next_index(Connections, Index),
      Worker = lists:nth(NextI, Connections),
      ets:insert(s7_pools_index, {Ip, NextI}),
%%      lager:info("~p found ~p connections, current ~p",[?MODULE, length(Connections), Worker]),
      {ok, Worker}
  end.

get_index(Key) ->
  case ets:lookup(s7_pools_index, Key) of
    [] -> 1;
    [{Key, Index}] -> Index
  end.

next_index(L, I) when I > length(L) -> length(L);
next_index(L, I) when I == length(L) -> 1;
next_index(_L, I) -> I + 1.