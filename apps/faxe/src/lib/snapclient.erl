%%%-------------------------------------------------------------------
%%% @author heyoka
%%% @copyright (C) 2019, <COMPANY>
%%% @doc
%%%
%%% @end
%%% @todo handle port exit_status -> reopen the port
%%% Created : 12. Jun 2019 11:28
%%%-------------------------------------------------------------------
-module(snapclient).
-author("heyoka").

-behaviour(gen_server).

%% API
-export([start_link/1, stop/1,
   connect_to/2, set_connection_type/2, set_connection_params/2,
   connect/1, disconnect/1, get_params/2, set_params/3, start/1]).

-export([
   read_area/2, write_area/2,
   db_read/2, db_write/2,
   ab_read/2, ab_write/2,
   eb_read/2, eb_write/2,
   mb_read/2, mb_write/2,
   tm_read/2, tm_write/2,
   ct_read/2, ct_write/2,
   read_multi_vars/2, write_multi_vars/2,
   get_plc_date_time/1,
   get_cpu_info/1, get_cp_info/1, get_plc_status/1]).

-export([set_session_password/2, clear_session_password/1, get_protection/1, list_blocks/1]).

%% gen_server callbacks
-export([init/1,
   handle_call/3,
   handle_cast/2,
   handle_info/2,
   terminate/2,
   code_change/3]).

-define(SERVER, ?MODULE).
-define(C_TIMEOUT, 3500).
-define(BLOCK_TYPES, [
   {ob, 16#38},
   {db, 16#41},
   {sdb, 16#42},
   {fc, 16#43},
   {sfc, 16#44},
   {fb, 16#45},
   {sfb, 16#46}
   ]).
-define(CONNECTION_TYPES, [
   {pg, 16#01},
   {op, 16#02},
   {s7_basic, 16#03}
   ]).
-define(AREA_TYPES, [
   {pe, 16#81}, %% process inputs
   {pa, 16#82}, %% process outputs
   {mk, 16#83}, %% markers
   {db, 16#84}, %% DB (data block)
   {ct, 16#1C}, %% count
   {tm, 16#1D} %% timer
   ]).
-define(WORD_TYPES, [
   {bit, 16#01},
   {byte, 16#02},
   {word, 16#04},
   {d_word, 16#06},
   {real, 16#08},
   {counter, 16#1C},
   {timer, 16#1D}
   ]).


-record(state, {
   port = nil,
   controlling_process = nil,
   queued_messages = [],
   ip = nil,
   rack = nil,
   slot = nil,
   state =  nil,
   is_active = false
}).

-type connect_opt() ::
   {ip, binary}
   | {rack, 0..7}
   | {slot, 1..31}
   | {local_tsap, integer}
   | {remote_tsap, integer}.


-type data_io_opt() ::
   {area, atom}
   | {db_number, integer}
   | {start, integer}
   | {amount, integer}
   | {word_len, atom}
   | {data, binary}.

-type data_io_map() :: #{}.

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @end
%%--------------------------------------------------------------------
-spec start(Opts :: list()) -> {ok, Pid :: pid()} | ignore | {error, Reason :: term()}.
start(Opts) ->
   gen_server:start(?MODULE, Opts, []).
-spec(start_link(Opts :: list()) ->
   {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link(Opts) ->
   gen_server:start_link(?MODULE, Opts, []).


-spec stop(pid()) -> any().
stop(Pid) ->
   gen_server:stop(Pid).

%% @doc
%% Connect to a S7 server.
%%  The following options are available:
%%
%%    * `:active` - (`true` or `false`) specifies whether data is received as
%%       messages or by calling "Data I/O functions".
%%
%%    * `:ip` - (string) PLC/Equipment IPV4 Address (e.g., "192.168.0.1")
%%
%%    * `:rack` - (int) PLC Rack number (0..7).
%%
%%    * `:slot` - (int) PLC Slot number (1..31).
%%
%%  For more info see pg. 96 form Snap7 docs.
%%
-spec connect_to(pid(), [connect_opt()]) -> ok | {error,term()}.
connect_to(Pid, Opts) when is_pid(Pid), is_list(Opts) ->
   gen_server:call(Pid, {connect_to, Opts}).


%% @doc
%% Sets the connection resource type, i.e the way in which the Clients connects to a PLC.
%%
-spec set_connection_type(pid(), atom()) -> ok | {error,term()}.
set_connection_type(Pid, ConnectionType) ->
   gen_server:call(Pid, {set_connection_type, ConnectionType}).


%% @doc
%% Sets internally (IP, LocalTSAP, RemoteTSAP) Coordinates
%%  The following options are available:
%%
%%    * `ip` - (string) PLC/Equipment IPV4 Address (e.g., "192.168.0.1")
%%
%%    * `local_tsap` - (int) Local TSAP (PC TSAP) // 0.
%%
%%    * `remote_tsap` - (int) Remote TSAP (PLC TSAP) // 0.
%%
-spec set_connection_params(pid(), [connect_opt()]) -> ok | {error,term()}.
set_connection_params(Pid, Opts) ->
   gen_server:call(Pid, {set_connection_params, Opts}).

%% @doc
%% Connects the client to the PLC with the parameters specified in the previous call of
%%  `connect_to/2` or `set_connection_params/2`.
%%
-spec connect(pid()) -> ok | {error,term()}.
connect(Pid) ->
   gen_server:call(Pid, connect).

%% @doc
%% Disconnects “gracefully” the Client from the PLC.
%%
-spec disconnect(pid()) -> ok | {error,term()}.
disconnect(Pid) ->
   gen_server:call(Pid, disconnect).


%% @doc
%% Reads an internal Client object parameter.
%%  For more info see pg. 89 form Snap7 docs.
%%
-spec get_params(pid(), integer()) -> ok | {error,term()}.
get_params(Pid, ParamNumber) ->
   gen_server:call(Pid, {get_params, ParamNumber}).


%% @doc
%% Sets an internal Client object parameter.
%%
-spec set_params(pid(), integer(), integer()) -> ok | {error, einval} | {error,term()}.
set_params(Pid, ParamNumber, Value) ->
   gen_server:call(Pid, {set_params, ParamNumber, Value}).


%%% DATA I/O functions



%% @doc
%% Reads a data area from a PLC.
%%  The following options are available:
%%
%%    * `:area` - (atom) Area Identifier (see @area_types).
%%
%%    * `:db_number` - (int) DB number, if `area: :DB` otherwise is ignored.
%%
%%    * `:start` - (int) An offset to start.
%%
%%    * `:amount` - (int) Amount of words to read/write.
%%
%%    * `:word_len` - (atom) Word size (see @word_types).
%%
%%  For more info see pg. 104 form Snap7 docs.
%%
-spec read_area(pid(), [data_io_opt()]) -> ok | {error, einval} | {error,term()}.
read_area(Pid, Opts) ->
   gen_server:call(Pid, {read_area, Opts}).


%% @doc
%% Write a data area from a PLC.
%%  The following options are available:
%%
%%    * `:area` - (atom) Area Identifier (see @area_types).
%%
%%    * `:db_number` - (int) DB number, if `area: :DB` otherwise is ignored.
%%
%%    * `:start` - (int) An offset to start.
%%
%%    * `:amount` - (int) Amount of words to read/write.
%%
%%    * `:word_len` - (atom) Word size (see @word_types).
%%
%%    * `:data` - (atom) buffer to write.
%%
%%  For more info see pg. 104 form Snap7 docs.
%%
-spec write_area(pid(), [data_io_opt()]) -> ok | {error, einval} | {error,term()}.
write_area(Pid, Opts) ->
   gen_server:call(Pid, {write_area, Opts}).



%% @doc
%% This is a lean function of read_area/2 to read PLC DB.
%%  It simply internally calls read_area/2 with
%%    * `area: :DB`
%%    * `word_len: :byte`
%%
%%  The following options are available:
%%
%%    * `:db_number` - (int) DB number (0..16#FFFF).
%%
%%    * `:start` - (int) An offset to start.
%%
%%    * `:amount` - (int) Amount of words (bytes) to read/write.
%%
%%  For more info see pg. 104 form Snap7 docs.
%%
-spec db_read(pid(), [data_io_opt()]) -> {ok, binary()} | {error, einval} | {error,term()}.
db_read(Pid, Opts) ->
   gen_server:call(Pid, {db_read, Opts}).


%% @doc
%% This is a lean function of write_area/2 to write PLC DB.
%%  It simply internally calls read_area/2 with
%%    * `area: :DB`
%%    * `word_len: :byte`
%%
%%  The following options are available:
%%
%%    * `:db_number` - (int) DB number (0..16#FFFF).
%%
%%    * `:start` - (int) An offset to start.
%%
%%    * `:amount` - (int) Amount of words (bytes) to read/write.
%%
%%    * `:data` - (bitstring) buffer to write.
%%
%%  For more info see pg. 104 form Snap7 docs.
%%
-spec db_write(pid(), [data_io_opt()]) -> ok | {error, einval} | {error,term()}.
db_write(Pid, Opts) ->
   gen_server:call(Pid, {db_write, Opts}).



%% @doc
%% This is a lean function of read_area/2 to read PLC process outputs.
%%  It simply internally calls read_area/2 with
%%    * `area: :PA`
%%    * `word_len: :byte`
%%
%%  The following options are available:
%%
%%    * `:start` - (int) An offset to start.
%%
%%    * `:amount` - (int) Amount of words (bytes) to read/write .
%%
%%  For more info see pg. 104 form Snap7 docs.
%%
-spec ab_read(pid(), [data_io_opt()]) -> {ok, binary()} | {error, einval} | {error,term()}.
ab_read(Pid, Opts) ->
   gen_server:call(Pid, {ab_read, Opts}).


%% @doc
%% This is a lean function of write_area/2 to write PLC process outputs.
%%  It simply internally calls read_area/2 with
%%    * `area: :PA`
%%    * `word_len: :byte`
%%
%%  The following options are available:
%%
%%    * `:start` - (int) An offset to start.
%%
%%    * `:amount` - (int) Amount of words (bytes) to read/write.
%%
%%    * `:data` - (bitstring) buffer to write.
%%
%%  For more info see pg. 104 form Snap7 docs.
%%
-spec ab_write(pid(), [data_io_opt()]) -> ok | {error, einval} | {error,term()}.
ab_write(Pid, Opts) ->
   gen_server:call(Pid, {ab_write, Opts}).


%% @doc
%% This is a lean function of read_area/2 to read PLC process inputs.
%%  It simply internally calls read_area/2 with
%%    * `area: :PE`
%%    * `word_len: :byte`
%%
%%  The following options are available:
%%
%%    * `:start` - (int) An offset to start.
%%
%%    * `:amount` - (int) Amount of words (bytes) to read/write .
%%
%%  For more info see pg. 104 form Snap7 docs.
%%
-spec eb_read(pid(), [data_io_opt()]) -> {ok, binary()} | {error, einval} | {error,term()}.
eb_read(Pid, Opts) ->
   gen_server:call(Pid, {eb_read, Opts}).


%% @doc
%% This is a lean function of write_area/2 to write PLC process inputs.
%%  It simply internally calls read_area/2 with
%%    * `area: :PE`
%%    * `word_len: :byte`
%%
%%  The following options are available:
%%
%%    * `:start` - (int) An offset to start.
%%
%%    * `:amount` - (int) Amount of words (bytes) to read/write.
%%
%%    * `:data` - (bitstring) buffer to write.
%%
%%  For more info see pg. 104 form Snap7 docs.
%%
-spec eb_write(pid(), [data_io_opt()]) -> ok | {error, einval} | {error,term()}.
eb_write(Pid, Opts) ->
   gen_server:call(Pid, {eb_write, Opts}).


%% @doc
%% This is a lean function of read_area/2 to read PLC merkers.
%%  It simply internally calls read_area/2 with
%%    * `area: :MK`
%%    * `word_len: :byte`
%%
%%  The following options are available:
%%
%%    * `:start` - (int) An offset to start.
%%
%%    * `:amount` - (int) Amount of words (bytes) to read/write .
%%
%%  For more info see pg. 104 form Snap7 docs.
%%
-spec mb_read(pid(), [data_io_opt()]) -> {ok, binary()} | {error, einval} | {error,term()}.
mb_read(Pid, Opts) ->
   gen_server:call(Pid, {mb_read, Opts}).


%% @doc
%% This is a lean function of write_area/2 to write PLC merkers.
%%  It simply internally calls read_area/2 with
%%    * `area: :MK`
%%    * `word_len: :byte`
%%
%%  The following options are available:
%%
%%    * `:start` - (int) An offset to start.
%%
%%    * `:amount` - (int) Amount of words (bytes) to read/write.
%%
%%    * `:data` - (bitstring) buffer to write.
%%
%%  For more info see pg. 104 form Snap7 docs.
%%
-spec mb_write(pid(), [data_io_opt()]) -> ok | {error, einval} | {error,term()}.
mb_write(Pid, Opts) ->
   gen_server:call(Pid, {mb_write, Opts}).


%% @doc
%% This is a lean function of read_area/2 to read PLC Timers.
%%  It simply internally calls read_area/2 with
%%    * `area: :TM`
%%    * `word_len: :timer`
%%
%%  The following options are available:
%%
%%    * `:start` - (int) An offset to start.
%%
%%    * `:amount` - (int) Amount of words (bytes) to read/write .
%%
%%  For more info see pg. 104 form Snap7 docs.
%%
-spec tm_read(pid(), [data_io_opt()]) -> {ok, binary()} | {error, einval} | {error,term()}.
tm_read(Pid, Opts) ->
   gen_server:call(Pid, {tm_read, Opts}).


%% @doc
%% This is a lean function of write_area/2 to write PLC Timers.
%%  It simply internally calls read_area/2 with
%%    * `area: :TM`
%%    * `word_len: :timer`
%%
%%  The following options are available:
%%
%%    * `:start` - (int) An offset to start.
%%
%%    * `:amount` - (int) Amount of words (bytes) to read/write.
%%
%%    * `:data` - (bitstring) buffer to write.
%%
%%  For more info see pg. 104 form Snap7 docs.
%%
-spec tm_write(pid(), [data_io_opt()]) -> ok | {error, einval} | {error,term()}.
tm_write(Pid, Opts) ->
   gen_server:call(Pid, {tm_write, Opts}).


%% @doc
%% This is a lean function of read_area/2 to read PLC Counters.
%%  It simply internally calls read_area/2 with
%%    * `area: :CT`
%%    * `word_len: :timer`
%%
%%  The following options are available:
%%
%%    * `:start` - (int) An offset to start.
%%
%%    * `:amount` - (int) Amount of words (bytes) to read/write .
%%
%%  For more info see pg. 104 form Snap7 docs.
%%
-spec ct_read(pid(), [data_io_opt()]) -> {ok, binary()} | {error, einval} | {error,term()}.
ct_read(Pid, Opts) ->
   gen_server:call(Pid, {ct_read, Opts}).


%% @doc
%% This is a lean function of write_area/2 to write PLC Counters.
%%  It simply internally calls read_area/2 with
%%    * `area: :CT`
%%    * `word_len: :timer`
%%
%%  The following options are available:
%%
%%    * `:start` - (int) An offset to start.
%%
%%    * `:amount` - (int) Amount of words (bytes) to read/write.
%%
%%    * `:data` - (bitstring) buffer to write.
%%
%%  For more info see pg. 104 form Snap7 docs.
%%
-spec ct_write(pid(), [data_io_opt()]) -> ok | {error, einval} | {error,term()}.
ct_write(Pid, Opts) ->
   gen_server:call(Pid, {ct_write, Opts}).



%% @doc
%% This function allows to read different kind of variables from a PLC in a single call.
%%  With it can read DB, inputs, outputs, Merkers, Timers and Counters.
%%
%%  The following options are available:
%%
%%    * `data` - (list of maps) a list of requests (maps with @data_io_opt options as keys) to read from PLC.
%%
%%  For more info see pg. 119 form Snap7 docs.
%%
-spec read_multi_vars(pid(), list(data_io_map())) -> {ok, binary()} | {error, einval} | {error,term()}.
read_multi_vars(Pid, Opts) ->
   gen_server:call(Pid, {read_multi_vars, Opts}).

%% @doc
%% This function allows to write different kind of variables from a PLC in a single call.
%%  With it can read DB, inputs, outputs, Merkers, Timers and Counters.
%%
%%  The following options are available:
%%
%%    * `data` - (list of maps) a list of requests (maps with @data_io_opt options as keys) to read from PLC.
%%
%%  For more info see pg. 119 form Snap7 docs.
%%
-spec write_multi_vars(pid(), list(data_io_map())) -> ok | {error, einval} | {error,term()}.
write_multi_vars(Pid, Opts) ->
   gen_server:call(Pid, {write_multi_vars, Opts}).

%% directory functions
-spec list_blocks(pid()) -> {ok, list()} | {error, term()}.
list_blocks(Pid) ->
   gen_server:call(Pid, list_blocks).


%% @doc
%% Reads PLC date and time, if successful, returns `{ok, Date, Time}`
%%
-spec get_plc_date_time(pid()) -> {ok, term(), term()} | {error, einval} | {error, term()}.
get_plc_date_time(Pid) ->
   gen_server:call(Pid, get_plc_date_time).

%% @doc
%% Gets CPU module name, serial number and other info.
%%
-spec get_cpu_info(pid()) -> {ok, list()} | {error, einval} | {error, term()}.
get_cpu_info(Pid) ->
   gen_server:call(Pid, get_cpu_info).

%% @doc
%% Gets CP (communication processor) info.
%%
-spec get_cp_info(pid()) -> {ok, list()} | {error, einval} | {error, term()}.
get_cp_info(Pid) ->
   gen_server:call(Pid, get_cp_info).


%% @doc
%% Returns the CPU status (running/stoppped).
%%
-spec get_plc_status(pid()) -> {ok, list()} | {error, einval} | {error, term()}.
get_plc_status(Pid) ->
   gen_server:call(Pid, get_plc_status).


%%% Security Functions

%% @doc
%% Send the password (an 8 chars string) to the PLC to meet its security level.
%%
-spec set_session_password(pid(), binary()) -> ok | {error, einval} | {error, term()}.
set_session_password(Pid, Password) ->
   gen_server:call(Pid, {set_session_password, Password}).


%% @doc
%% Clears the password set for the current session (logout).
%%
-spec clear_session_password(pid()) -> ok | {error, einval} | {error, term()}.
clear_session_password(Pid) ->
   gen_server:call(Pid, clear_session_password).


%% @doc
%% Gets the CPU protection level info.
%%
-spec get_protection(pid()) -> ok | {error, einval} | {error, term()}.
get_protection(Pid) ->
   gen_server:call(Pid, get_protection).

%%% Misc Functions




%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
-spec(init(Args :: term()) ->
   {ok, State :: #state{}} | {ok, State :: #state{}, timeout() | hibernate} |
   {stop, Reason :: term()} | ignore).
init([]) ->
   Port = port_open(),
   {ok, #state{port = Port}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @end
%%--------------------------------------------------------------------



handle_call({connect_to, Opts}, {FromPid, _}, State) ->
   Ip = proplists:get_value(ip, Opts),
   Rack = proplists:get_value(rack, Opts, 0),
   Slot = proplists:get_value(slot, Opts, 0),
   Active = proplists:get_value(active, Opts, false),

   Response = call_port(State, connect_to, {Ip, Rack, Slot}),

   NewState = case Response of
                 ok -> State#state{
                    state = connected, ip = Ip,
                    rack = Rack, slot = Slot,
                    is_active = Active, controlling_process = FromPid};
                 {error, _W} -> State#state{state = idle}
              end,

   {reply, Response, NewState};

handle_call({set_connection_type, ConnType}, _From, State) ->
   case proplists:get_value(ConnType, ?CONNECTION_TYPES, undefined) of
      undefined -> error({ConnType, is_not_a_valid_connection_type} );
      ConnectionType -> Response = call_port(State, set_connection_type, ConnectionType),
         {reply, Response, State}
   end;

handle_call({set_connection_params, Opts}, _From, State) ->
   Ip = proplists:get_value(ip, Opts),
   LocalTSAP = proplists:get_value(local_tsap, Opts, 0),
   RemoteTSAP = proplists:get_value(remote_tsap, Opts, 0),
   Response = call_port(State, set_connection_params, {Ip, LocalTSAP, RemoteTSAP}),
   {reply, Response, State};

handle_call(connect, _From, State) ->
   Resp = call_port(State, connect, nil),
   NewState = case Resp of
                 ok -> State#state{state = connected};
                 {error, _X} -> State#state{state = idle}
              end,
   {reply, Resp, NewState};

handle_call(disconnect, _From, State) ->
   Resp = call_port(State, disconnect, nil),
   %% @ todo close port ?
   {reply, Resp, State#state{state = idle}};

handle_call({get_params, ParamNumber}, _From, State) ->
   Resp = call_port(State, get_params, ParamNumber),
   {reply, Resp, State};

handle_call({set_params, ParamNumber, Value}, _From, State) ->
   Resp = call_port(State, set_params,{ParamNumber, Value}),
   {reply, Resp, State};

%%% DATA I/O functions

handle_call({read_area, Opts}, _From, State) ->
   AreaKey = proplists:get_value(area, Opts),
   WordLenKey = proplists:get_value(word_len, Opts, byte),
   DbNumber = proplists:get_value(db_number, Opts, 0),
   Start = proplists:get_value(start, Opts, 0),
   Amount = proplists:get_value(amount, Opts, 0),
   AreaType = proplists:get_value(AreaKey, ?AREA_TYPES),
   WordType = proplists:get_value(WordLenKey, ?WORD_TYPES),
   Response = call_port(State, read_area, {AreaType, DbNumber, Start, Amount, WordType}),
   {reply, Response, State};


handle_call({write_area, Opts}, _From, State) ->
   AreaKey = proplists:get_value(area, Opts),
   WordLenKey = proplists:get_value(word_len, Opts, byte),
   DbNumber = proplists:get_value(db_number, Opts, 0),
   Start = proplists:get_value(start, Opts, 0),
   Data = proplists:get_value(data, Opts),
   Amount = proplists:get_value(amount, Opts, 0),
   AreaType = proplists:get_value(AreaKey, ?AREA_TYPES),
   WordType = proplists:get_value(WordLenKey, ?WORD_TYPES),
   Response = call_port(State, write_area, {AreaType, DbNumber, Start, Amount, WordType, Data}),
   {reply, Response, State};


handle_call({db_read, Opts}, _From, State) ->
   DbNumber = proplists:get_value(db_number, Opts, 0),
   Start = proplists:get_value(start, Opts, 0),
   Amount = proplists:get_value(amount, Opts, 0),
   Response = call_port(State, db_read, {DbNumber, Start, Amount}),
   {reply, Response, State};


handle_call({read_multi_vars, Opts}, _From, State) ->
   Data = lists:map(fun(Map) -> key2value(Map) end, Opts),
   Size = length(Data),
   Response = call_port(State, read_multi_vars, {Size, Data}),
   {reply, Response, State};


handle_call({write_multi_vars, Opts}, _From, State) ->
   Data = lists:map(fun(Map) -> key2value(Map) end, Opts),
   Size = length(Data),
   Response = call_port(State, write_multi_vars, {Size, Data}),
   {reply, Response, State};


%%%%%%%%
% other IO missing for the moment
%%%%%%%%

%%%%% directory

handle_call(list_blocks, _From, State) ->
   Res = call_simple(State, list_blocks),
   {reply, Res, State};

%%%%%

handle_call(get_plc_date_time, _From, State) ->
   Response = call_port(State, get_plc_date_time, nil),
   {reply, Response, State};


%%%
%% some are missing
%%%%


handle_call(get_cpu_info, _From, State) ->
   Resp = call_simple(State, get_cpu_info),
   Res =
   case Resp of
      {ok, Data} -> {ok, [{Key, io_lib:write_string(binary_to_list(Val))} || {Key, Val} <- Data]};
      _ -> Resp
   end,
   {reply, Res, State};

handle_call(get_cp_info, _From, State) ->
   Response = call_port(State, get_cp_info, nil),
   {reply, Response, State};

%% call all simple commands with no params
handle_call({call, Command}, _From, State) ->
   {reply, call_simple(State, Command), State};

handle_call(_Request, _From, State) ->
   {reply, ok, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @end
%%--------------------------------------------------------------------
-spec(handle_cast(Request :: term(), State :: #state{}) ->
   {noreply, NewState :: #state{}} |
   {noreply, NewState :: #state{}, timeout() | hibernate} |
   {stop, Reason :: term(), NewState :: #state{}}).
handle_cast(_Request, State) ->
   {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
-spec(handle_info(Info :: timeout() | term(), State :: #state{}) ->
   {noreply, NewState :: #state{}} |
   {noreply, NewState :: #state{}, timeout() | hibernate} |
   {stop, Reason :: term(), NewState :: #state{}}).
handle_info({Port,{exit_status, Status}}, State=#state{port = Port}) ->
   lager:error("Port: ~p exited with status: ~p" ,[Port, Status]),
%%   NewPort = port_open(),
   {stop, port_exited, State};
%%   {noreply, State#state{port = NewPort}};
handle_info({'EXIT', Port, PosixCode}, State) ->
   lager:warning("Port: ~p exited with Code: ~p", [Port, PosixCode]),
   {stop, port_exited, State};
handle_info(_Info, State) ->
   {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
-spec(terminate(Reason :: (normal | shutdown | {shutdown, term()} | term()),
    State :: #state{}) -> term()).
terminate(_Reason, State=#state{port = Port}) ->
   catch erlang:port_close(Port).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
-spec(code_change(OldVsn :: term() | {down, term()}, State :: #state{},
    Extra :: term()) ->
   {ok, NewState :: #state{}} | {error, Reason :: term()}).
code_change(_OldVsn, State, _Extra) ->
   {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
port_open() ->
   Snap7Dir = code:priv_dir(faxe),
   os:putenv("LD_LIBRARY_PATH", Snap7Dir),
   os:putenv("DYLD_LIBRARY_PATH", Snap7Dir),
   Executable = Snap7Dir ++ "/s7_client.o",
   Port = open_port({spawn_executable, Executable}, [
      {args, []},
      {packet, 2},
      use_stdio,
      binary,
      exit_status
   ]),
   lager:info("Port: ~p, from ~p is ALIVE?: ~p",[Port, Executable, is_port(Port)]),
   Port.

call_simple(State, Command) ->
   call_port(State, Command, nil).

call_port(State, Command, Args) ->
   call_port(State, Command, Args, ?C_TIMEOUT).
call_port(_State = #state{port = Port}, Command, Args, Timeout) ->
   Msg = {Command, Args},
%%   ok = send_data(Port, {command, erlang:term_to_binary(Msg)}),
   erlang:send(Port, {self(), {command, erlang:term_to_binary(Msg)}}),
   % Block until the response comes back since the C side
   % doesn't want to handle any queuing of requests. REVISIT
   receive
      {_, {data, <<114, Response/binary>>}} -> binary_to_term(Response)
   after
      Timeout ->
         % Not sure how this can be recovered
         exit(port_timed_out)
   end.

%%-spec send_data(Port::port(), Data::binary()) -> ok | error.
%%send_data(Port, Data) when is_port(Port) ->
%%   try port_command(Port, Data) of
%%      true ->
%%         ok
%%   catch
%%      error:badarg ->
%%         error
%%   end.


key2value(Opts) ->
   AreaKey = maps:get(area, Opts),
   AreaValue = proplists:get_value(AreaKey, ?AREA_TYPES),
   Map = maps:put(area, AreaValue, Opts),
   WordLenKey = maps:get(word_len, Opts, byte),
   WordLenValue = proplists:get_value(WordLenKey, ?WORD_TYPES),
   maps:put(word_len, WordLenValue, Map).

