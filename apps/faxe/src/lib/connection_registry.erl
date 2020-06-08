%%%-------------------------------------------------------------------
%%% @author heyoka
%%% @copyright (C) 2020, <COMPANY>
%%% @doc
%%% @end
%%%-------------------------------------------------------------------
-module(connection_registry).

-behaviour(gen_server).

-export([start_link/0, connected/0, disconnected/0, connecting/0, reg/4]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
  code_change/3]).

-include("faxe.hrl").

-define(SERVER, ?MODULE).
-define(DATA_FORMAT, <<"92.003">>).
%%
%% status: 0 = disconnected, 1 = connected, 2 = connecting
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
reg({_FlowId, _NodeId} = Id, Peer, Port, Type) ->
  ?SERVER ! {reg, self(), Id, Peer, Port, Type}.

connecting() ->
  ?SERVER ! {connecting, self()}.

connected() ->
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

handle_info({reg, Client, {FlowId, NodeId} = _Id, Peer, Port, Type} = _R, State=#state{clients = Clients}) ->
  erlang:monitor(process, Client),
  Peer1 =
  case is_list(Peer) of
    true -> list_to_binary(Peer);
    false -> Peer
  end,
  ets:insert(node_connections,
    {Client, #conreg{peer = Peer1, port = Port, flowid = FlowId, nodeid = NodeId, conn_type = Type}}),
  {noreply, State#state{clients = [Client|Clients]}};
handle_info({connecting, Client} = _R, State) ->
  Con = get_connection(Client),
  case Con#conreg.status of
    connecting ->
      ok;
    _ ->
      out(Client, Con#conreg{connected = false, status = 2})
  end,
  {noreply, State};
handle_info({connected, Client } = _R, State) ->
  Con = get_connection(Client),
  case Con#conreg.status of
    1 ->
      ok;
    _ ->
      out(Client, Con#conreg{connected = true, status = 1})
  end,
  {noreply, State};
handle_info({disconnected, Client} = _R, State) ->
  Con = get_connection(Client),
  case Con#conreg.status of
    0 ->
      ok;
    _ ->
      out(Client, Con#conreg{connected = false, status = 0})
  end,
  {noreply, State};
handle_info({'DOWN', _Mon, process, Pid, _Info}, State = #state{clients = Clients}) ->
  case lists:member(Pid, Clients) of
    true ->
      Con = get_connection(Pid),
      remove_node_entries(Pid),
      publish(Con#conreg{status = 0, connected = false});
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

out(Client, Con=#conreg{}) ->
  write(Client, Con),
  publish(Con).

write(Client, Con=#conreg{}) ->
  ets:insert(node_connections, {Client, Con}).

publish(#conreg{connected = Connected, status = Status, flowid = FId,
  nodeid = NId, peer = Peer, port = Port, conn_type = Type}) ->
  Fields = #{
    <<"connected">> => Connected,
    <<"status">> => Status,
    <<"flow_id">> => FId,
    <<"node_id">> => NId,
    <<"peer">> => Peer,
    <<"port">> => Port,
    <<"conn_type">> => Type,
    <<"df">> => ?DATA_FORMAT
  },
  P = #data_point{ts = faxe_time:now(), fields = Fields},
  gen_event:notify(conn_status, {{FId, NId}, P}).