-module(atom_table).

-export([count/0]).

count() ->
   Info      = erlang:system_info(info),
   Chunks    = binary:split(Info, <<"=">>, [global]),
   [TabInfo] = [X || <<"index_table:atom_tab", X/binary>> <- Chunks],
   Lines     = binary:split(TabInfo, <<"\n">>, [global]),
   Chunks2   = [binary:split(L, <<": ">>) || L <- Lines, L =/= <<>>],
   lists:foldl(
      fun([EName, EVal], Acc) ->
         case EName of
            <<"entries">> -> binary_to_integer(EVal);
            _ -> Acc
         end
      end,
      undefined,
      Chunks2
   ).
%%   binary_to_integer(proplists:get_value(<<"entries">>, Chunks2)).