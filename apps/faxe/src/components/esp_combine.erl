%% Date: 05.01.17 - 14:11
%% Ⓒ 2017 heyoka
%%
%% @doc
%% Combine the values of 2 nodes
%%
%% Port 1 is the trigger port,
%% every time a value is received on the trigger port, the node will emit a value, combined with
%% whatever current value on port 2.
%%
%% The node will never emit on port 2 values,
%% it will only emit, when a value is received on port 1 (the trigger port).
%%
%% No output is given, if there has never arrived a value on port 2 to combine with.
%%
%% @end
-module(esp_combine).
-author("Alexander Minichmair").


%% API
-behavior(df_component).

-include("faxe.hrl").
%% API
-export([init/3, process/3, options/0, inports/0]).

-record(state, {
   node_id,
   fields,
   row_buffer = undefined,
   row_aliases = [],
   prefix,
   name_param
}).

inports() ->
   [
      {1, 1},
      {2, 1}
   ].

options() -> [
   {combined, node, {port, 2}},
   {fields, string_list, []},
   {tags, string_list, undefined},
   {aliases, string_list, undefined},
   {prefix, binary, undefined}].


init(NodeId, _Ins, #{fields := Fields, aliases := Aliases, prefix := Prefix}=Ps) ->
   lager:debug("~p init:node~p",[NodeId, Ps]),
   NP =
      case Prefix of
         undefined -> Aliases;
         _ when is_binary(Prefix) -> Prefix
      end,
   {ok, all, #state{fields = Fields, node_id = NodeId, row_aliases = Aliases, prefix = Prefix, name_param = NP}}.



%% trigger port
process(1, #data_point{} = _Point, State = #state{row_buffer = undefined}) ->
   {ok, State};
process(1, #data_point{} = Point, State = #state{fields = Fs, row_buffer = Buffer, name_param = NP}) ->
   Combined = combine(Point, Buffer, Fs, NP),
   lager:info("DataPoint in on port 1 : ~p ::: ~p",[Point, Combined]),
   {emit, Combined, State}
;
process(2, #data_point{} = Point, State = #state{}) ->
   lager:info("DataPoint in on port 2 : ~p",[Point]),
   {ok, State#state{row_buffer = Point}}
;

process(1, #data_batch{}, State = #state{}) ->
   {ok, State}.


combine(Point=#data_point{}, SPoint=#data_point{}, Fields, Prefix) when is_binary(Prefix) ->
   N = fun(Param, FName) ->
      <<Param/binary, <<".">>/binary, FName/binary>>
      end,
   trans(Point, SPoint, Fields, N, Prefix);
combine(Point=#data_point{}, SPoint=#data_point{}, Fields, Names) when is_list(Names) ->
   N = fun(Param, FName) ->
      proplists:get_value(FName, Param)
       end,
   trans(Point, SPoint, Fields, N, Names).


trans(Point, SPoint, Fields, ToNameFun, Params) ->
   lists:foldl(
      fun(FName, P) ->
         FVal = flowdata:value(SPoint, FName),
         flowdata:set_field(P, ToNameFun(Params, FName), FVal)
      end,
      Point,
      Fields
   ).