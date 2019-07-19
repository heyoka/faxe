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
-export([init/3, process/3, options/0, handle_info/2, shutdown/1, maybe_emit/4]).

-record(state, {
  ip,
  port,
  client,
  slot,
  rack,
  interval,
  as,
  diff,
  timer_ref,
  vars, var_types :: list(),
  last_values = [] :: list()
}).

options() -> [
  {ip, binary},
  {port, integer, 102},
  {every, binary, "1s"},
  {slot, integer, 0},
  {rack, integer, 0},
  {vars, string_list}, %% s7 addressing, ie: DB2024,Int16.1224
  {as, binary_list},
  {diff, is_set}].

init(_NodeId, _Ins,
    #{ip := Ip,
      port := Port,
      every := Dur,
      slot := Slot,
      rack := Rack,
      vars := Addresses,
      as := As,
      diff := Diff} = Opts) ->

  lager:notice("++++ ~p ~ngot opts: ~p ~n",[_NodeId, Opts]),

  Client = connect(Ip, Rack, Slot),

  ParamList = [s7addr:parse(Address) || Address <- Addresses],
  Splitted = [maps:take(dtype, Map) || Map <- ParamList],
  TypeList = [K || {K, _P} <- Splitted],
  PList = [P || {_K, P} <- Splitted],

  lager:notice("ParameterList from Addresses string: ~p ~n~n~p ~n~n~p~n~n~p",[ParamList, Splitted, TypeList, PList]),
  Interval = faxe_time:duration_to_ms(Dur),
  TRef = poll(0),
  {ok, all,
    #state{
      ip = Ip,
      port = Port,
      as = lists:zip(As, TypeList),
      slot = Slot,
      rack = Rack,
      client = Client,
      interval = Interval,
      timer_ref = TRef,
      diff = Diff,
      vars = PList,
      var_types = TypeList}}.

process(_In, #data_batch{points = _Points} = _Batch, State = #state{}) ->
  {ok, State};
process(_Inport, #data_point{} = _Point, State = #state{}) ->
  {ok, State}.

handle_info(poll,
    State=#state{client = Client, interval = Interval, as = Aliases,
      vars = Opts, diff = Diff, last_values = LastList, ip = Ip, rack = Rack, slot = Slot}) ->
  NewState =
  case (catch snapclient:read_multi_vars(Client, Opts)) of
    {ok, Res} -> lager:info("Result from snap7 polling ~p : ~p",[Opts, Res]),
      maybe_emit(Diff, Res, Aliases, LastList), State#state{last_values = Res};
    _Other -> NewClient = connect(Ip, Rack, Slot), State#state{client = NewClient}
  end,
  TRef = poll(Interval),
  {ok, NewState#state{timer_ref = TRef}};
%% client process is down, we match the Object field from the DOWN message against the current client pid
handle_info({'DOWN', _MonitorRef, _Type, Client, Info},
    State=#state{client = Client, ip = Ip, rack = Rack, slot = Slot}) ->
  lager:warning("Snap7 Client process is DOWN with : ~p ! ", [Info]),
  NewClient = connect(Ip, Rack, Slot),
  {ok, State#state{client = NewClient}};
%% old DOWN message from alread restarted client process
handle_info({'DOWN', _MonitorRef, _Type, _Object, _Info}, State) ->
  {ok, State};
handle_info(E, S) ->
  lager:warning("unexpected: ~p~n", [E]),
  {ok, S#state{}}.

shutdown(#state{client = Client, timer_ref = Timer}) ->
  catch (erlang:cancel_timer(Timer)),
  catch (snapclient:disconnect(Client)).

poll(Interval) ->
  lager:notice("new poll timeout with interval: ~p",[Interval]),
  erlang:send_after(Interval, self(), poll).


-spec maybe_emit(Diff :: true|false, ResultList :: list(), Aliases :: list(), LastResults :: list()) -> ok | term().
%% no diff flag -> emit
maybe_emit(false, Res, Aliases, _) ->
  Out = build_point(Res, Aliases),
  lager:notice("~p emitting no diff (or empty last list): ~p ",[?MODULE, Out]),
  dataflow:emit(Out);
%% no last values -> emit
maybe_emit(true, Res, Aliases, []) ->
  maybe_emit(false, Res, Aliases, []);
%% diff flag and result-list is exactly last list -> no emit
maybe_emit(true, Result, _, Result) ->
  lager:warning("diff is true and there is no diff in ResultList !"),
  ok;
%% diff flag -> emit diff values only
maybe_emit(true, Result, Aliases, LastList) ->
  ResAliasList = lists:zip(Aliases, Result),
  LastAliasList = lists:zip(Aliases, LastList),
  lager:info("resalias: ~p ~n lastalias : ~p",[ResAliasList, LastAliasList]),
  ResList = lists:filter(
    fun({K, Res}) -> (proplists:get_value(K, LastAliasList) /= Res) end,
    ResAliasList),
  {ResAliases, ResValues} = lists:unzip(ResList),
  lager:info("unzipped again: ~p" ,[lists:unzip(ResList)]),
  Out = build_point(ResValues, ResAliases),
  lager:notice("~p emitting diff: ~p ",[?MODULE, Out]),
  dataflow:emit(Out).


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
  <<Res:8/binary>> = Data,
  Res; %% maybe to_string ?
decode(int, Data) ->
  <<Res:16/integer-signed>> = Data,
  Res;
decode(d_int, Data) ->
  <<Res:32/integer-signed>> = Data,
  Res;
decode(word, Data) ->
  <<Res:16/float-unsigned>> = Data,
  Res;
decode(d_word, Data) ->
  <<Res:32/float-unsigned>> = Data,
  Res;
decode(float, Data) ->
  <<Res:32/float-signed>> = Data,
  Res;
decode(_, Data) -> Data.

connect(Ip, Rack, Slot) ->
  {ok, Client} = snapclient:start([]),
  erlang:monitor(process, Client),
  ok = snapclient:connect_to(Client, [{ip, Ip}, {slot, Slot}, {rack, Rack}]),
  Client.
