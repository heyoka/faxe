%%%-------------------------------------------------------------------
%%% @author heyoka
%%% @copyright (C) 2019, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 23. Okt 2019 20:05
%%%-------------------------------------------------------------------
-module(s7reader).
-author("heyoka").

-behaviour(gen_server).

%% API
-export([start_link/1]).

%% gen_server callbacks
-export([init/1,
   handle_call/3,
   handle_cast/2,
   handle_info/2,
   terminate/2,
   code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {
   ip,
   port,
   client,
   slot,
   rack,
   as,
   vars,
   parent
}).

%%%===================================================================
%%% API
%%%===================================================================

-spec(start_link(map()) ->
   {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link(Opts) ->
   gen_server:start_link(?MODULE, Opts, []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

-spec(init(Args :: term()) ->
   {ok, State :: #state{}} | {ok, State :: #state{}, timeout() | hibernate} |
   {stop, Reason :: term()} | ignore).
init(#{ip := Ip,
   port := Port,
   slot := Slot,
   rack := Rack,
   vars := Addresses}) ->
   Client = connect(Ip, Rack, Slot),
   {ok, #state{ip = Ip, port = Port, slot = Slot, rack = Rack, vars = Addresses, client = Client}}.

handle_call(_Request, _From, State) ->
   {reply, ok, State}.

-spec(handle_cast(Request :: term(), State :: #state{}) ->
   {noreply, NewState :: #state{}} |
   {noreply, NewState :: #state{}, timeout() | hibernate} |
   {stop, Reason :: term(), NewState :: #state{}}).
handle_cast(_Request, State) ->
   {noreply, State}.

handle_info(read,
    State=#state{client = Client, vars = Opts, ip = Ip, rack = Rack, slot = Slot, parent = Parent}) ->
      NewState =
         case (catch snapclient:read_multi_vars(Client, Opts)) of
            {ok, Res} -> Parent ! {read_ok, self(), Res}, State;
            _Other -> lager:warning("Error when reading S7 Vars: ~p", [_Other]),
               NewClient = connect(Ip, Rack, Slot),
               Parent ! {read_error, self(), []},
               State#state{client = NewClient}

         end,
   {noreply, NewState};
%% client process is down, we match the Object field from the DOWN message against the current client pid
handle_info({'DOWN', _MonitorRef, _Type, Client, Info},
    State=#state{client = Client, ip = Ip, rack = Rack, slot = Slot}) ->
   lager:warning("Snap7 Client process is DOWN with : ~p ! ", [Info]),
   NewClient = connect(Ip, Rack, Slot),
   {ok, State#state{client = NewClient}};
%% old DOWN message from already restarted client process
handle_info({'DOWN', _MonitorRef, _Type, _Object, _Info}, State) ->
   {ok, State};
handle_info(_E, S) ->
   {ok, S#state{}}.


-spec(terminate(Reason :: (normal | shutdown | {shutdown, term()} | term()),
    State :: #state{}) -> term()).
terminate(_Reason, _State) ->
   ok.
-spec(code_change(OldVsn :: term() | {down, term()}, State :: #state{},
    Extra :: term()) ->
   {ok, NewState :: #state{}} | {error, Reason :: term()}).
code_change(_OldVsn, State, _Extra) ->
   {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
connect(Ip, Rack, Slot) ->
   {ok, Client} = snapclient:start([]),
   erlang:monitor(process, Client),
   ok = snapclient:connect_to(Client, [{ip, Ip}, {slot, Slot}, {rack, Rack}]),
   Client.