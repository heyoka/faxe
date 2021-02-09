%%%-------------------------------------------------------------------
%%% @author heyoka
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%% bounded in-memory queue
%%% @end
%%% Created : 08. Feb 2021 18:53
%%%-------------------------------------------------------------------
-module(memory_queue).
-author("heyoka").
-include("faxe.hrl").

%% API
-export([enq/2, deq/1, to_list/1, new/1, to_list_reset/1, new/0]).


-spec new() -> #mem_queue{}.
new() ->
   Size = faxe_config:get(memory_queue_max_size),
   new(Size).

-spec new(non_neg_integer()) -> #mem_queue{}.
new(Size) ->
   #mem_queue{q = queue:new(), max = Size, current = 0}.

-spec enq(term(), #mem_queue{}) -> #mem_queue{}.
enq(NewItem, Q=#mem_queue{q = Queue, max = MaxQLen, current = Len}) ->
   {NewQ, NewQLen} =
      case Len >= MaxQLen of
         true -> {queue:drop(Queue), Len - 1};
         false -> {Queue, Len}
      end,
%%   lager:notice("mem_q: old_len: ~p, new_len: ~p", [Len, NewQLen+1]),
   QueueNew = queue:in(NewItem, NewQ),
%%   [lager:info("~p",[E]) || E <- queue:to_list(QOut) ],
   Q#mem_queue{q = QueueNew, current = NewQLen + 1}.

-spec deq(#mem_queue{}) -> {ok, term(), #mem_queue{}} | {empty, #mem_queue{}}.
deq(Q=#mem_queue{q = Queue, current = Len}) ->
   case queue:out(Queue) of
      {{value, Item}, Queue2 }  ->
         {ok, Item, Q#mem_queue{q = Queue2, current = Len - 1}};
      {empty, _Queue1} ->
         {empty, Q}
   end.

-spec to_list(#mem_queue{}) -> list().
to_list(#mem_queue{q = Queue}) ->
   queue:to_list(Queue).

-spec to_list_reset(#mem_queue{}) -> {list(), #mem_queue{}}.
to_list_reset(Q = #mem_queue{q = Queue}) ->
   {queue:to_list(Queue),
      Q#mem_queue{q = queue:new(), current = 0}
   }.


