%%%-------------------------------------------------------------------
%%% @author heyoka
%%% @copyright (C) 2022, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 11. Jul 2022 19:05
%%%-------------------------------------------------------------------
-module(process_stats).
-author("heyoka").

-include("faxe.hrl").

-define(PYTHON_COMP, c_python3).

%% API
-export([get_top_reds/0, get_top_reds/1, get_top_nodes/1, get_top_msgq/1, get_top_python_nodes/1]).

get_top_reds() ->
  get_top_reds(5).

get_top_reds(N) ->
  AllProcs = erlang:processes(),
  top_list(AllProcs, reductions, N).

get_top_msgq(N) ->
  AllProcs = erlang:processes(),
  top_list(AllProcs, message_queue_len, N).

get_top_nodes(N) ->
  SortBy = reductions,
  AllGraphs = faxe:list_running_tasks(),
  GraphNodes = lists:foldl(
    fun(#task{pid = GPid, name = GraphName}, Acc) ->
      GraphNodes = df_graph:nodes(GPid),
      Acc ++ lists:map(fun({node,NodeName,NodePid,_,_}) -> {NodePid, <<GraphName/binary, "-", NodeName/binary>>} end, GraphNodes)
    end,
    [],
    AllGraphs
  ),
  AllInfo = lists:map(fun({P, Name}) -> {Name, erlang:process_info(P)} end, GraphNodes),
%%  lager:notice("AllInfo ~p",[AllInfo]),
  Sorted = lists:usort(fun({_NA, A}, {_NB, B}) -> proplists:get_value(SortBy, A) > proplists:get_value(SortBy, B) end, AllInfo),
%%  lager:notice("Sorted ~p",[Sorted]),
  TopList = lists:sublist(Sorted, N),
  F =
    fun({Name, E}) ->
      M = maps:from_list(E),
      Reduced =#{initial_call := InitalCall} = maps:with([registered_name,initial_call,reductions,message_queue_len], M),
      Reduced#{initial_call => tuple_to_list(InitalCall), <<"node">> => Name}
    end,
  lists:map(F, TopList).

get_top_python_nodes(N) ->
  AllGraphs = faxe:list_running_tasks(),
  GraphNodes = lists:foldl(
    fun(#task{pid = GPid, name = GraphName}, Acc) ->
      GraphNodes0 = df_graph:nodes(GPid),
      GraphNodes = lists:filter(
        fun
          ({node,_NodeName,_NodePid,?PYTHON_COMP,_}) -> true;
          (_) -> false
        end,
        GraphNodes0),

      Acc ++
      lists:map(fun({node,NodeName,NodePid,_,_}=E) ->
%%        lager:info("~p", [E]),
        {NodePid, <<GraphName/binary, "-", NodeName/binary>>} end, GraphNodes)
    end,
    [],
    AllGraphs
  ),
  AllInfo = lists:map(
    fun({P, Name}) ->
      {ok, Stats} = gen_server:call(P, get_stats),
      {Name, Stats}
    end,
    GraphNodes),
  AllInfo,
  Sorted = lists:usort(fun({_NA, A}, {_NB, B}) ->
    proplists:get_value(<<"mem">>, A) > proplists:get_value(<<"mem">>, B) end, AllInfo),
  TopList = lists:sublist(Sorted, N),
  F = fun({Name, E}) -> #{<<"node">> => Name, <<"stats">> => maps:from_list(E)} end,
  lists:map(F, TopList).

top_list(Processes, SortBy, N) ->
  AllInfo = lists:map(fun(P) -> erlang:process_info(P) end, Processes),
  Sorted = lists:usort(fun(A, B) -> proplists:get_value(SortBy, A) > proplists:get_value(SortBy, B) end, AllInfo),
  TopList = lists:sublist(Sorted, N),
%%  lager:notice("TopList: ~p",[TopList]),
  F =
    fun(E) ->
      M = maps:from_list(E),

      Reduced = #{dictionary := Dict, initial_call := InitCall, current_function := CFun} =
        maps:with([registered_name,total_heap_size,dictionary,initial_call,reductions,message_queue_len,memory,current_function], M),
      InitialCall = proplists:get_value('$initial_call', Dict, InitCall),
      Meta = maps:from_list(proplists:get_value('__lager_metadata', Dict, [])),
      R = maps:without([dictionary],
        Reduced#{initial_call => mfa_to_bin(InitialCall), current_function => mfa_to_bin(CFun), meta => Meta}),
%%      lager:info("map form is ~p",[R]),
      R
    end,
  lists:map(F, TopList).

mfa_to_bin(undefined) ->
  <<"na">>;
mfa_to_bin({Module, Function, Arity}) ->
  list_to_binary(faxe_util:to_list(Module) ++ ":" ++ faxe_util:to_list(Function) ++ "/" ++ integer_to_list(Arity)).
