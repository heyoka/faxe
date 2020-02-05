%% Date: 05.02.20 - 20:22
%% Ⓒ 2020 heyoka
%% @doc
%% Persist value(s) that are available in lambda expression within a faxe flow
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
   {field, string},
   {type, string, <<"single">>},
   {key, string, undefined},
   {default, any, undefined}].

check_options() ->
   [
      {one_of, type, [<<"single">>, <<"set">>, <<"list">>]}
   ].

init(_NodeId, _Ins, #{field := Field, key := As0, type := Type, default := Def}) ->
   As =
   case As0 of
      undefined -> Field;
      _ -> As0
   end,
   default(Type, As, Def),
   {ok, all, #state{field = Field, key = As, type = Type}}.

process(_Inport, Item, State = #state{}) ->
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
   Set0 =
      case ets:lookup(ls_mem_set, MemKey) of
         [] -> sets:new();
         [{MemKey, List}] -> sets:from_list(List)
      end,
   Set = sets:add_element(flowdata:value(P, MemField), Set0),
   ets:insert(ls_mem_set, {MemKey, sets:to_list(Set)}).

ls_mem_list(#data_batch{points = Points}, State=#state{}) ->
   [ls_mem_list(P, State) || P <- Points];
ls_mem_list(P = #data_point{}, #state{key = MemKey, field = MemField}) ->
   L0 =
      case ets:lookup(ls_mem_list, MemKey) of
         [] -> [];
         [{MemKey, List0}] -> List0
      end,
   List = [flowdata:value(P, MemField)] ++ L0,
   ets:insert(ls_mem_list, {MemKey, List}).


default(<<"single">>, Key, Def) ->
   ets:insert(ls_mem, {Key, def(<<"single">>, Def)});
default(<<"set">>, Key, Def) ->
   ets:insert(ls_mem_set, {Key, def(<<"set">>, Def)});
default(<<"list">>, Key, Def) ->
   ets:insert(ls_mem_set, {Key, def(<<"list">>, Def)}).


def(<<"single">>, undefined) -> 0;
def(_, undefined) -> [];
def(<<"single">>, Val) -> Val;
def(_, Val) -> [Val].

