%%%-------------------------------------------------------------------
%%% @author heyoka
%%% @copyright (C) 2020, <COMPANY>
%%% @doc
%%% @end
%%%-------------------------------------------------------------------
-module(s7pool_manager).

-behaviour(gen_server).

-export([start_link/0, connect/1, read_vars/2, get_pdu_size/1]).
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
  case s7pool_con_handler:get_connection(Ip) of
    {ok, Worker} ->
%%      lager:notice("got connection ~p ",[Worker]),
      s7worker:get_pdu_size(Worker);
    Other ->
      Other
  end.

read_vars(_Opts=#{ip := Ip}, Vars) ->
  case s7pool_con_handler:get_connection(Ip) of
    {ok, Worker} ->
      case catch gen_server:call(Worker, {read, Vars}) of
        {ok, _} = R -> R;
        _Nope ->
          lager:warning("error when reading : ~p",[_Nope]),
          {error, read_failed}
      end;
    Other ->
      Other
  end.

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
  lager:notice("ensure_pool for ip :~p for user: ~p",[Ip, User]),
  erlang:monitor(process, User),
  NewPUsers = add_user(Ip, PUsers, User),
  IpDemand = check_demand(Ip, NewPUsers),
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
  case lists:member(Ip, Up) of
    true -> {noreply, State};
    false ->
      [U ! {s7_connected, Ip} || U <- maps:get(Ip, UWaiting)],
      {noreply, State#state{pools_up = [Ip|Up]}}
  end;
handle_info({down, Ip}, State = #state{pools_up = Up, ips_pools = _Pools, pool_user = PoolUsers}) ->
  case maps:is_key(Ip, PoolUsers) of
    true ->
      UsersIp = maps:get(Ip, PoolUsers),
      [U ! {s7_disconnected, Ip} || U <- sets:to_list(UsersIp)];
    false -> ok
  end,
  {noreply, State#state{pools_up = lists:delete(Ip, Up)}};
handle_info({'EXIT', Pid, Why}, State = #state{pools_ips = Pools, ips_pools = Ips, ip_opts = IpOpts}) ->
  NewState =
  case maps:is_key(Pid, Pools) of
    true ->
      Ip = maps:get(Pid, Pools),
      lager:warning("Pool-Handler ~p-~p is down: ~p",[Pid, Ip, Why]),
      Opts = maps:get(Ip, IpOpts),
      NewPools = maps:without([Pid], Pools),
      NewIps = maps:without([Ip], Ips),
      {ok, NewPid} = s7pool_handler:start_link(Opts),
      State#state{ips_pools = NewIps#{Ip => NewPid}, pools_ips = NewPools#{NewPid => Ip}};
    false ->
      State
  end,
  {noreply, NewState};
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
    true -> Handler = maps:get(Ip, Ips), Handler ! {demand, check_demand(Ip, NewPoolUsers)};
    false -> ok
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

check_demand(Ip, AllUsers) ->
  case maps:is_key(Ip, AllUsers) of
    true ->
      PUsersSet = maps:get(Ip, AllUsers),
      sets:size(PUsersSet);
    false ->
      0
  end.
