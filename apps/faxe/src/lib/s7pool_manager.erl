%%%-------------------------------------------------------------------
%%% @author heyoka
%%% @copyright (C) 2020, <COMPANY>
%%% @doc
%%% s7 pool main module
%%% @end
%%%-------------------------------------------------------------------
-module(s7pool_manager).

-behaviour(gen_server).

-export([start_link/0, connect/1, read_vars/2, get_pdu_size/1, connection_count/1, get_connection/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
  code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {
  pools_ips = #{},
  ips_pools = #{},
  ip_opts = #{},
  pool_user = #{},
  pools_up = [],
  users_waiting = #{}
}).

%%%===================================================================
%%% Spawning and gen_server implementation
%%%===================================================================
connect(Opts) ->
  ?SERVER ! {ensure_pool, Opts, self()}.

get_pdu_size(Ip) ->
  case get_connection(Ip) of
    {ok, Worker} ->
%%      lager:notice("got connection ~p ",[Worker]),
      s7worker:get_pdu_size(Worker);
    Other ->
      Other
  end.

read_vars(_Opts=#{ip := Ip}, Vars) ->
  case get_connection(Ip) of
    {ok, Worker} ->
      case catch gen_server:call(Worker, {read, Vars}) of
        {ok, _} = R -> R;
        Nope ->
          lager:warning("~p error when reading multivars ~p",[?MODULE, Nope]),
          {error, read_failed}
      end;
    Other ->
      Other
  end.

connection_count(Ip) ->
  case ets:lookup(s7_pools, Ip) of
    [] -> 0;
    [{Ip, []}] -> 0;
    [{Ip, Connections}] -> length(Connections)
  end.

%%%===================================================================
%%% get a connection from a pool
%%%===================================================================
get_connection(Key) ->
  Index = get_index(Key),
  get_connection(Key, Index).



start_link() ->
  gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

init([]) ->
  erlang:process_flag(trap_exit, true),
  {ok, #state{}}.

handle_call(_Request, _From, State = #state{}) ->
  {reply, ok, State}.

handle_cast(_Request, State = #state{}) ->
  {noreply, State}.

handle_info({ensure_pool, #{ip := Ip} = Opts, User},
    State = #state{ips_pools = Ips, pools_ips = Pools, ip_opts = IpOpts, pool_user = PUsers,
      users_waiting = UsersWaiting, pools_up = Up}) ->
  lager:notice("ensure_pool for ip :~p for user: ~p, current connection count: ~p",[Ip, User, connection_count(Ip)]),
  erlang:monitor(process, User),
  NewPUsers = add_user(Ip, PUsers, User),
  IpDemand = check_demand(Ip, NewPUsers),
  lager:info("Demand for IP ~p is ~p",[Ip, IpDemand]),
  {NewState, PoolHandler} =
  case maps:is_key(Ip, Ips) of
    true ->
      {State, maps:get(Ip, Ips)};
    false ->
      {ok, Pid} = s7pool_handler:start_link(Opts),
      {State#state{
        ips_pools = Ips#{Ip => Pid},
        pools_ips = Pools#{Pid => Ip},
        ip_opts = IpOpts#{Ip => Opts}},
        Pid
      }
  end,
  UWaiting =
    case lists:member(Ip, Up) of
      true ->
        User ! {s7_connected, Ip},
        UsersWaiting;
      false ->
        case maps:is_key(Ip, UsersWaiting) of
          true -> UsersWaiting#{Ip => [User| maps:get(Ip, UsersWaiting)]};
          false -> UsersWaiting#{Ip => [User]}
        end
    end,
%%  lager:notice("[~p] ips_pools: ~p ~n pools_ips:~p ~n ip_opts: ~p",
%%    [?MODULE, NewState#state.ips_pools, NewState#state.pools_ips, NewState#state.ip_opts]),
  PoolHandler ! {demand, IpDemand},
  {noreply, NewState#state{pool_user = NewPUsers, users_waiting = UWaiting}};

handle_info({up, Ip}, State = #state{pools_up = Up, ips_pools = _Pools, users_waiting = UWaiting}) ->
  lager:info("pool for ip ~p is UP",[Ip]),
  case lists:member(Ip, Up) of
    true -> {noreply, State};
    false ->
      inform_users(Ip, s7_connected, State),
      [U ! {s7_connected, Ip} || U <- maps:get(Ip, UWaiting)],
      {noreply, State#state{pools_up = [Ip|Up]}}
  end;
handle_info({down, Ip}, State = #state{pools_up = Up, ips_pools = _Pools}) ->
  lager:info("pool for ip ~p is DOWN",[Ip]),
  inform_users(Ip, s7_disconnected, State),
  {noreply, State#state{pools_up = lists:delete(Ip, Up)}};
handle_info({'EXIT', Pid, normal}, State = #state{}) ->
  lager:warning("Pool-Handler exiting normal, will no restart: ~p",[Pid]),
  NewState = remove_handler(Pid, State),
  {noreply, NewState};
%% handler exited
handle_info({'EXIT', Pid, Why}, State = #state{pools_ips = Pools, ip_opts = IpOpts})  when is_map_key(Pid, Pools) ->
  Ip = maps:get(Pid, Pools),
  lager:warning("Pool-Handler ~p-~p is down: ~p",[Pid, Ip, Why]),
  NState = do_remove_handler(Pid, Ip, State),
  Opts = maps:get(Ip, IpOpts),
  {ok, NewPid} = s7pool_handler:start_link(Opts),
  {noreply, NState#state{
    ips_pools = (NState#state.ips_pools)#{Ip => NewPid},
    pools_ips = (NState#state.pools_ips)#{NewPid => Ip}}
  };
handle_info({'EXIT', _Pid, _Why}, State = #state{}) ->
  {noreply, State};
%% pool-user is DOWN
handle_info({'DOWN', _Mon, process, Pid, _Info}, State = #state{pool_user =  PoolUsers, ips_pools = Ips}) ->
%%  lager:notice("[~p] pool-user is down: ~p", [?MODULE, Pid]),
%%  lager:info("PoolUsers: ~p", [PoolUsers]),
  F = fun({Ip, UserList}, {L, LastIp}) ->
    UList = sets:to_list(UserList),
        case lists:member(Pid, UList) of
          true -> { [{Ip, sets:from_list(lists:delete(Pid, UList))}|L] , Ip};
          false -> { [{Ip, sets:from_list(UList)}|L], LastIp }
        end
      end,
  {NewPoolUsers0, Ip} = lists:foldl(F, {[], 0}, maps:to_list(PoolUsers)),
  NewPoolUsers = maps:from_list(NewPoolUsers0),
%%  lager:info("PoolUsers after: ~p",[NewPoolUsers]),
  case maps:is_key(Ip, Ips) of
    true ->
      Handler = maps:get(Ip, Ips),
      Handler ! {demand, check_demand(Ip, NewPoolUsers)};
    false ->
      lager:warning("no pool handler found for ~p",[Ip]),
      ok
  end,
  {noreply, State#state{pool_user = NewPoolUsers}};
handle_info(Req, State) ->
  lager:warning("Unhandled Request : ~p",[Req]),
  {noreply, State}.

terminate(_Reason, _State = #state{}) ->
  ok.

code_change(_OldVsn, State = #state{}, _Extra) ->
  {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
remove_handler(HandlerPid, State = #state{pools_ips = Pools}) when is_map_key(HandlerPid, Pools) ->
  Ip = maps:get(HandlerPid, Pools),
  do_remove_handler(HandlerPid, Ip, State);
remove_handler(_HandlerPid, State = #state{}) ->
  State.


do_remove_handler(HandlerPid, Ip, State = #state{pools_ips = Pools, ips_pools = Ips}) ->
  NewPools = maps:without([HandlerPid], Pools),
  NewIps = maps:without([Ip], Ips),
  State#state{ips_pools = NewIps, pools_ips = NewPools}.

inform_users(Ip, StateMsg, #state{pool_user = PoolUsers}) when is_map_key(Ip, PoolUsers) ->
  UsersIp = maps:get(Ip, PoolUsers),
  [U ! {StateMsg, Ip} || U <- sets:to_list(UsersIp)];
inform_users(_Ip, _StateMsg, #state{}) ->
  ok.

add_user(Ip, AllUsers, NewUser) ->
  PoolUsers =
  case maps:is_key(Ip, AllUsers) of
    true ->
      PUsersSet = maps:get(Ip, AllUsers),
      sets:add_element(NewUser, PUsersSet);
    false ->
      sets:from_list([NewUser])
  end,
  AllUsers#{Ip => PoolUsers}.

check_demand(Ip, AllUsers) when is_map_key(Ip, AllUsers) ->
  PUsersSet = maps:get(Ip, AllUsers),
  sets:size(PUsersSet);
check_demand(_Ip, _AllUsers) ->
  0.




%%%===================================================================
%%% connection functions
%%%===================================================================
get_connection(Ip, Index) ->
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
