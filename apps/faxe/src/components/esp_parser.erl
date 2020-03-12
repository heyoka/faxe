%%%-------------------------------------------------------------------
%%% @author heyoka
%%% @copyright (C) 2019, <COMPANY>
%%% @doc
%%% This node connects to a tcp endpoint and awaits data in a special format, which is defined
%%% by the parser parameter, the parser will then try to convert the data to faxe format and emit the
%%% result
%%% At the moment tcp messages must start with a 2 byte header denoting the length of the following data.
%%% << Length_Header:16/integer, Data:{Length_Header}/binary >>
%%% If the 'changes' option is given, the node will only emit on changed values
%%%
%%% The tcp listener is protected against flooding with the {active, once} inet option
%%% @end
%%% Created : 27. May 2019 09:00
%%%-------------------------------------------------------------------
-module(esp_parser).
-author("heyoka").

%% API
-behavior(df_component).

-include("faxe.hrl").
%% API
-export([init/3, process/3, options/0, handle_info/2]).

-record(state, {
  field,
  as,
  parser = undefined :: undefined|atom(), %% parser module
  changes = false,
  prev_crc32
}).

options() -> [
  {field, binary},
  {as, binary, <<"data">>}, %% alias for fieldname
  {parser, atom, undefined}, %% parser module to use
  {changed, is_set, false} %% only emit, when new data is different to previous
].


init(_NodeId, _Ins,
    #{field := Field, as := As, parser := Parser, changed := Changed}) ->

  {ok, all,
    #state{field = Field, as = As,  changes = Changed, parser = Parser}}.

process(_In, #data_batch{points = _Points} = _Batch, State = #state{}) ->
  {error, State};
process(_Inport, #data_point{} = Point, State = #state{field = Field}) ->
  maybe_emit(Point, flowdata:value(Point, Field), State).

handle_info(_E, S) ->
  {ok, S}.

maybe_emit(Point, Data, State = #state{changes = false}) -> do_process(Point, Data, State);
maybe_emit(Point, Data, State = #state{changes = true, prev_crc32 = undefined}) ->
  DataCheckSum = erlang:crc32(Data),
  NewState = State#state{prev_crc32 = DataCheckSum},
  do_process(Point, Data, NewState);
maybe_emit(Point, Data, State = #state{changes = true, prev_crc32 = PrevCheckSum}) ->
  DataCheckSum = erlang:crc32(Data),
  NewState = State#state{prev_crc32 = DataCheckSum},
  case  DataCheckSum /= PrevCheckSum of
    true -> do_process(Point, Data, NewState);
    false -> {ok, NewState}
  end.

do_process(DataPoint, Data, State = #state{as = As, parser = Parser}) ->
  case (catch binary_msg_parser:convert(Data, DataPoint, As, Parser)) of
    P when is_record(P, data_point) ->
      {emit, P, State};
    Err -> lager:warning("Parsing error [~p] ~nmessage ~p",[Parser, Err]),
      {ok, State}
  end.



