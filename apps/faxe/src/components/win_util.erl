%% Date: 06.01.17 - 19:29
%% Ⓒ 2017 heyoka
-module(win_util).
-author("Alexander Minichmair").

%% API
-export([sync/2, split/2]).

%% @doc
%% head-drop as many entries in the first list, as there are in the second
%% return the first, maybe shorthend list
%% @end
-spec sync(list(), list()) -> list().
sync(List, []) ->
   List;
sync([_LH|L], [_E|R]) ->
   sync(L, R).

%% @doc
%% provide a list of timestamps and a reference timestamp
%% -> and get back a list of timestamps, which are older than the reference
%% and a list of Timestamps which a are younger or equally old
-spec split(list(), non_neg_integer()) -> {Keep :: list(), Evict :: list()}.
split(List, Pred) ->
%%   lager:notice("Pred in split_ea : ~p ", [Pred]),
   split_ea(List, Pred, [], [], false).

%% find old/outdated items, keep rest
-spec split_ea(
    In :: list(), Pred :: non_neg_integer(), Keep :: list(), Evict :: list(), Done :: true|false)
       -> {Keep :: list(), Evict :: list()}.

split_ea([], _, Keep, Evict, _) ->
   {Keep, Evict};
split_ea([Ts|T] = List, Pred, Keep, Evict, false) ->
   {K, Ev, Done} =
      case Ts > Pred of
         true -> {List, Evict, true};
         false ->  lager:debug("~p evict ~p",[?MODULE, Ts]), {Keep, [Ts|Evict], false}

      end,
   split_ea(T, Pred, K, Ev, Done);
split_ea(_, _, Keep, Evict, true) ->
   {Keep, Evict}.


