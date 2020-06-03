%%%-------------------------------------------------------------------
%%% @author heyoka
%%% @copyright (C) 2020, <COMPANY>
%%% @doc
%%% @end
%%%-------------------------------------------------------------------
-module(connection_registry).

-behaviour(gen_server).

-export([start_link/0, connected/0, disconnected/0, connecting/0, reg/3]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
  code_change/3]).

-define(SERVER, ?MODULE).

-record(conreg, {
  status = 0,
  connected = false,
  flowid,
  nodeid,
  peer,
  port,
  conn_type,
  meta

}).

-record(state, {
  clients = [],
  flow_conns = #{},
  flows = []
}).

%%%===================================================================
%%% Spawning and gen_server implementation
%%%===================================================================
reg({_FlowId, _NodeId} = Id, Peer, Port) ->
  ?SERVER ! {reg, self(), Id, Peer, Port}.

connecting() ->
%%  lager:notice("Id: ~p, Peer: ~p, Port: ~p",[Id, Peer, Port]),
  ?SERVER ! {connecting, self()}.

connected() ->
%%  lager:notice("Id: ~p, Peer: ~p, Port: ~p",[Id, Peer, Port]),
  ?SERVER ! {connected, self()}.

disconnected() ->
  ?SERVER ! {disconnected, self()}.

start_link() ->
  gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

init([]) ->
  {ok, #state{}}.

handle_call(_Request, _From, State = #state{}) ->
  {reply, ok, State}.

handle_cast(_Request, State = #state{}) ->
  {noreply, State}.

handle_info({reg, Client, {FlowId, NodeId} = _Id, Peer, Port} =R, State=#state{clients = Clients}) ->
  erlang:monitor(process, Client),
  ets:insert(node_connections, {Client, #conreg{peer = Peer, port = Port, flowid = FlowId, nodeid = NodeId}}),
  {noreply, State#state{clients = [Client|Clients]}};
handle_info({connecting, Client} =R, State) ->
  Con = get_connection(Client),
  case Con#conreg.status of
    connecting ->
      lager:info("already connecting ...",[]),
      ok;
    _ ->
      lager:info("~p connecting !!!",[Con]),
      %backend ! {connected, {Id, Peer, Port}},
      ets:insert(node_connections, {Client, Con#conreg{connected = false, status = 2}})
  end,
  {noreply, State};
handle_info({connected, Client }=R, State) ->
  lager:info("~p",[R]),
  Con = get_connection(Client),
  case Con#conreg.status of
    1 ->
      lager:info("already connected ...",[]),
      ok;
    _ ->
      lager:info("~p connected !!!",[Con]),
      %backend ! {connected, {Id, Peer, Port}},
      ets:insert(node_connections, {Client, Con#conreg{connected = true, status = 1}})
  end,
  {noreply, State};
handle_info({disconnected, Client} = R, State) ->
  lager:info("~p",[R]),
  Con = get_connection(Client),
  case Con#conreg.status of
    0 ->
      lager:info("already disconnected ...",[]);
    _ ->
      lager:info("~p disconnected !!!",[Con]),
      ets:insert(node_connections, {Client, Con#conreg{connected = false, status = 0}})
  end,
  {noreply, State};
handle_info({'DOWN', _Mon, process, Pid, _Info}, State = #state{clients = Clients}) ->
  case lists:member(Pid, Clients) of
    true ->
      remove_node_entries(Pid),
      lager:notice("watched flow is down: ~p",[Pid]);
    false ->
      lager:notice("flow is down but not watched?: ~p",[Pid])
  end,
  {noreply, State#state{clients = lists:delete(Pid, Clients)}};
handle_info(_Info, State = #state{}) ->
  {noreply, State}.

terminate(_Reason, _State = #state{}) ->
  ok.

code_change(_OldVsn, State = #state{}, _Extra) ->
  {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

get_connection(Pid) ->
  case ets:lookup(node_connections, Pid) of
    [{Pid, #conreg{}=C}] -> C;
    _ -> #conreg{connected = false}
  end.

remove_node_entries(Pid) ->
  ets:match_delete(node_connections, {Pid, '_'}).
