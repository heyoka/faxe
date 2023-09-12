%%%-------------------------------------------------------------------
%%% @author heyoka
%%% @copyright (C) 2019, <COMPANY>
%%% @doc
%%% The parser tries to convert any data to faxe's internal format and emit the resulting data-item.
%%% If the 'changes' option is given, the node will only emit on changed values
%%%
%%% @end
%%% Created : 27. May 2019 09:00
%%%-------------------------------------------------------------------
-module(esp_parser).
-author("heyoka").

%% API
-behavior(df_component).

-include("faxe.hrl").
%% API
-export([init/3, process/3, options/0, handle_info/2, emits/0, wants/0, check_options/0, parser_check/1]).

-record(state, {
  field,
  as,
  parser = undefined :: undefined|atom(), %% parser module
  changes = false,
  prev_crc32
}).

options() -> [
  {field, string},
  {as, string, <<"data">>}, %% alias for fieldname
  {parser, string}, %% parser module to use
  {changed, is_set, false} %% only emit, when new data is different to previous
].

check_options() ->
  [
    {func, parser,
      fun parser_check/1,
      <<" has not a valid parser-name">>
      }
  ].

parser_check(ParserName) when is_binary(ParserName) ->
  Pars = catch faxe_util:save_binary_to_atom(ParserName),
  case is_atom(Pars) of
    true -> erlang:function_exported(Pars, parse, 1);
    false -> false
  end;
parser_check(_) ->
  false.

wants() -> point.
emits() -> point.


init(_NodeId, _Ins,
    #{field := Field, as := As, parser := Parser0, changed := Changed}) ->
  Parser = faxe_util:save_binary_to_atom(Parser0),
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



