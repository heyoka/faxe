%%%-------------------------------------------------------------------
%%% @author heyoka
%%% @copyright (C) 2019, <COMPANY>
%%% @doc
%%% get data from a siemens s7 plc via the snap7 library
%%%
%%% @end
%%% Created : 14. June 2019 11:32:22
%%%-------------------------------------------------------------------
-module(esp_s7poll).
-author("heyoka").

%% API
-behavior(df_component).

-include("faxe.hrl").
%% API
-export([init/3, process/3, options/0, handle_info/2, shutdown/1, maybe_emit/4,
  check_options/0, split/2, build_addresses/1, build_point/2]).

-define(MAX_READ_ITEMS, 19).

-define(RECON_MIN_INTERVAL, 100).
-define(RECON_MAX_INTERVAL, 6000).
-define(RECON_MAX_RETRIES, infinity).

-record(state, {
  ip,
  port,
  client,
  slot,
  rack,
  interval,
  as,
  align,
  diff,
  timer_ref,
  vars, var_types :: list(),
  last_values = [] :: list(),
  timer :: #faxe_timer{},
  reconnector
}).

options() -> [
  {ip, binary},
  {port, integer, 102},
  {every, duration, "1s"},
  {align, is_set},
  {slot, integer, 0},
  {rack, integer, 0},
  {vars, string_list}, %% s7 addressing, ie: DB2024,Int16.1224 | DB2024.DBX12.2
  {as, binary_list},
  {diff, is_set}].

check_options() ->
  [{same_length, [vars, as]},{max_param_count, [vars], ?MAX_READ_ITEMS}].

init(_NodeId, _Ins,
    #{ip := Ip,
      port := Port,
      every := Dur,
      align := Align,
      slot := Slot,
      rack := Rack,
      vars := Addresses,
      as := As,
      diff := Diff}) ->

  Reconnector = faxe_backoff:new(
    {?RECON_MIN_INTERVAL, ?RECON_MAX_INTERVAL, ?RECON_MAX_RETRIES}),

  {PList, TypeList} = Ads = build_addresses(Addresses),
  lager:notice("Addresses: ~p", [Ads]),
  erlang:send_after(0, self(), do_reconnect),

  {ok, all,
    #state{
      ip = Ip,
      port = Port,
      as = lists:zip(As, TypeList),
      slot = Slot,
      rack = Rack,
      align = Align,
      interval = Dur,
      reconnector = Reconnector,
      diff = Diff,
      vars = PList,
      var_types = TypeList}}.

process(_In, #data_batch{points = _Points} = _Batch, State = #state{}) ->
  {ok, State};
process(_Inport, #data_point{} = _Point, State = #state{}) ->
  {ok, State}.

handle_info(poll,
    State=#state{client = Client, as = Aliases, timer = Timer,
      vars = Opts, diff = Diff, last_values = LastList}) ->

  case (catch snapclient:read_multi_vars(Client, Opts)) of
    {ok, Res} ->
      lager:notice("got form s7: ~p", [Res]),
      maybe_emit(Diff, Res, Aliases, LastList),
      NewTimer = faxe_time:timer_next(Timer),
      {ok, State#state{timer = NewTimer, last_values = Res}};
    _Other ->
      lager:warning("Error when reading S7 Vars: ~p", [_Other]),
      NewTimer = faxe_time:timer_cancel(Timer),
      try_reconnect(State#state{client = undefined, timer = NewTimer})
  end;
%% client process is down,
%% we match the Object field from the DOWN message against the current client pid
handle_info({'DOWN', _MonitorRef, _Type, Client, Info},
    State=#state{client = Client, timer = Timer}) ->
  lager:warning("Snap7 Client process is DOWN with : ~p ! ", [Info]),
  NewTimer = faxe_time:timer_cancel(Timer),
  try_reconnect(State#state{client = undefined, timer = NewTimer});
%% old DOWN message from already restarted client process
handle_info({'DOWN', _MonitorRef, _Type, _Object, _Info}, State) ->
  {ok, State};
handle_info(do_reconnect,
    State=#state{ip = Ip, port = Port, rack = Rack, slot = Slot, align = Align, interval = Dur}) ->
  lager:info("[~p] do_reconnect, ~p", [?MODULE, {Ip, Port}]),
  case connect(Ip, Rack, Slot) of
    {ok, Client} ->
      Timer = faxe_time:init_timer(Align, Dur, poll),
      {ok, State#state{client = Client, timer = Timer}};
    {error, Error} ->
      lager:error("[~p] Error connecting to PLC ~p: ~p",[?MODULE, {Ip, Port},Error]),
      try_reconnect(State)
  end;
handle_info(_E, S) ->
  {ok, S#state{}}.

shutdown(#state{client = Client, timer = Timer}) ->
  catch (faxe_time:timer_cancel(Timer)),
  catch (snapclient:disconnect(Client)),
  catch (snapclient:stop(Client)).

try_reconnect(State=#state{reconnector = Reconnector}) ->
  case faxe_backoff:execute(Reconnector, do_reconnect) of
    {ok, Reconnector1} ->
      {ok, State#state{reconnector = Reconnector1}};
    {stop, Error} -> logger:error("[Client: ~p] PLC reconnect error: ~p!",[?MODULE, Error]),
      {stop, {shutdown, Error}, State}
  end.

-spec maybe_emit(Diff :: true|false, ResultList :: list(), Aliases :: list(), LastResults :: list()) -> ok | term().
%% no diff flag -> emit
maybe_emit(false, Res, Aliases, _) ->
  Out = build_point(Res, Aliases),
  dataflow:emit(Out);
%% no last values -> emit
maybe_emit(true, Res, Aliases, []) ->
  maybe_emit(false, Res, Aliases, []);
%% diff flag and result-list is exactly last list -> no emit
maybe_emit(true, Result, _, Result) ->
%%  lager:warning("diff is true and there is no diff in ResultList !"),
  ok;
%% diff flag -> emit diff values only
maybe_emit(true, Result, Aliases, LastList) ->
  ResAliasList = lists:zip(Aliases, Result),
  LastAliasList = lists:zip(Aliases, LastList),
%%  lager:info("resalias: ~p ~n lastalias : ~p",[ResAliasList, LastAliasList]),
  ResList = lists:filter(
    fun({K, Res}) -> (proplists:get_value(K, LastAliasList) /= Res) end,
    ResAliasList),
  {ResAliases, ResValues} = lists:unzip(ResList),
%%  lager:info("unzipped again: ~p" ,[lists:unzip(ResList)]),
  Out = build_point(ResValues, ResAliases),
  dataflow:emit(Out).



build_addresses(Addresses) ->
  ParamList = [s7addr:parse(Address) || Address <- Addresses],
  Splitted = [maps:take(dtype, Map) || Map <- ParamList],
  TypeList = [K || {K, _P} <- Splitted],
  PList = [P || {_K, P} <- Splitted],
  {PList, TypeList}.

split([], _) -> [];
split(L, N) when length(L) < N -> [L];
split(L, N) ->
  {A, B} = lists:split(N, L),
  [A | split(B, N)].


build_point(ResultList, AliasList) when is_list(ResultList), is_list(AliasList) ->
  build(#data_point{ts=faxe_time:now()}, ResultList, AliasList).

build(Point=#data_point{}, [], []) ->
  Point;
build(Point=#data_point{}, [Res|R],[{Alias, DType}|A]) ->
  NewPoint = flowdata:set_field(Point, Alias, decode(DType, Res)),
  build(NewPoint, R, A).


decode(bool, Data) ->
  binary:decode_unsigned(Data);
decode(byte, Data) ->
  <<Res:8/binary>> = Data,
  Res;
decode(char, Data) ->
  <<Res:1/binary>> = Data,
  Res; %% maybe to_string ?
decode(int, Data) ->
  <<Res:16/integer-signed>> = Data,
  Res;
decode(d_int, Data) ->
  <<Res:32/integer-signed>> = Data,
  Res;
decode(word, Data) ->
  <<Res:16/unsigned>> = Data,
  Res;
decode(d_word, Data) ->
  <<Res:32/float-unsigned>> = Data,
  Res;
decode(float, Data) ->
  <<Res:32/float-signed>> = Data,
  Res;
decode(_, Data) -> Data.


connect(Ip, Rack, Slot) ->
  case catch do_connect(Ip, Rack, Slot) of
    Client when is_pid(Client) -> {ok, Client};
    Err -> {error, Err}
  end.

do_connect(Ip, Rack, Slot) ->
  {ok, Client} = snapclient:start([]),
  ok = snapclient:connect_to(Client, [{ip, Ip}, {slot, Slot}, {rack, Rack}]),
  erlang:monitor(process, Client),
  Client.
