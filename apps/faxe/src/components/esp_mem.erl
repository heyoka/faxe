%% Date: 05.02.20 - 20:22
%% Ⓒ 2020 heyoka
%% @doc
%% Flow wide value storage. mem values are available to any other node (in lambda expressions) within a flow.
%% There are 3 types of memories:
%% + 'single' holds a single value
%% + 'list' holds a list of values, value order is preserved within the list
%% + 'set' holds a list of values, where values have not duplicates
%% %% + 'map' holds a map of key-value pairs
%%
%% Values a gathered normally within the flow of data, but can also be pre-set with values
%% The values will be hold in an ets term storage
%% @end
-module(esp_mem).
-author("Alexander Minichmair").

%% API
-behavior(df_component).

-include("faxe.hrl").
%% API
-export([init/3, process/3, options/0, check_options/0]).

-define(TABLE, ls_mem).

-record(state, {
   field,
   key,
   type
}).

options() -> [
   {field, string, undefined},
   {type, string, <<"single">>},
   {key, string, undefined},
   {default, any, undefined},
   {default_json, is_set, false}
].

check_options() ->
   [
      {one_of, type, [<<"single">>, <<"set">>, <<"list">>]},
      {oneplus_of_params, [field, default]}
   ].

init(_NodeId, _Ins, #{field := Field, key := As0, type := Type, default := Def, default_json := DefJson}) ->
   As =
   case As0 of
      undefined -> Field;
      _ -> As0
   end,
   Default =
   case DefJson of true -> jiffy:decode(Def, [return_maps]); false -> Def end,
   default(Type, As, Default),
   {ok, all, #state{field = Field, key = As, type = Type}}.

%% if field is undefined the node is used as a lookup with initial data in it which should not be overwritten, we assume
process(_, _Item, State = #state{field = undefined}) ->
   % do nothing
   {ok, State};
process(_, Item, State = #state{}) ->
   mem(Item, State),
   {emit, Item, State}.

mem(Item, State = #state{type = <<"single">>}) ->
   ls_mem_single(Item, State);
mem(Item, State = #state{type = <<"list">>}) ->
   ls_mem_list(Item, State);
mem(Item, State = #state{type = <<"set">>}) ->
   ls_mem_set(Item, State).


ls_mem_single(#data_batch{points = Points}, State=#state{}) ->
   ls_mem_single(lists:last(Points), State);
ls_mem_single(P = #data_point{}, #state{key = MemKey, field = MemField}) ->
%%   lager:notice("handle_ls_mem: key: ~p field: ~p :: ~p", [MemKey, MemField, flowdata:value(P, MemField)]),
   ets:insert(ls_mem, {MemKey, flowdata:value(P, MemField)}).
%%   lager:warning("ls_mem: ~p is now: ~p", [MemKey, faxe_lambda_lib:ls_mem(MemKey)]).

ls_mem_set(#data_batch{points = Points}, State=#state{}) ->
   [ls_mem_set(P, State) || P <- Points];
ls_mem_set(P = #data_point{}, #state{key = MemKey, field = MemField}) ->
%%   lager:notice("handle_ls_mem_set"),
   Set0 = sets:from_list(ets_set_or_list(MemKey, ls_mem_set)),
   Set = sets:add_element(flowdata:value(P, MemField), Set0),
   ets:insert(ls_mem_set, {MemKey, sets:to_list(Set)}).

ls_mem_list(#data_batch{points = Points}, State=#state{}) ->
   [ls_mem_list(P, State) || P <- Points];
ls_mem_list(P = #data_point{}, #state{key = MemKey, field = MemField}) ->
   L0 = ets_set_or_list(MemKey, ls_mem_list),
   List = [flowdata:value(P, MemField)] ++ L0,
   ets:insert(ls_mem_list, {MemKey, List}).

ets_set_or_list(MemKey, Table) ->
   case ets:lookup(Table, MemKey) of
      [] -> [];
      [{MemKey, List0}] -> List0
   end.

default(<<"single">>, Key, Def) ->
   ets:insert(ls_mem, {Key, def(<<"single">>, Def)});
default(<<"set">>, Key, Def) ->
   ets:insert(ls_mem_set, {Key, def(<<"set">>, Def)});
default(<<"list">>, Key, Def) ->
   ets:insert(ls_mem_list, {Key, def(<<"list">>, Def)}).


def(<<"single">>, undefined) -> 0;
def(_, undefined) -> [];
def(<<"single">>, Val) -> Val;
def(_, Val) -> [Val].

