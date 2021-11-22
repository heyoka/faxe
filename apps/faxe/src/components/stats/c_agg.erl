%% Date: 12.05.17 - 20:50
%% Ⓒ 2017 heyoka
-module(c_agg).
-author("Alexander Minichmair").

-include("faxe.hrl").

%% API
-export([call/4]).


-spec call(tuple(), atom(), term(), binary()) -> #data_batch{} | #data_point{}.
call({[], _}, _Module, _MState, _As) ->
   #data_batch{};
call({Tss,_Vals}=Data, Module, MState, As) when is_list(Tss) ->
   result(Module:execute(Data, MState), As, Tss).

%%-spec result({Ts, V} | [{Ts, V}], binary(), list()) -> #data_point{} | #data_batch{}.
result({first, Value}, As, Tss) ->
   Timestamp = lists:last(Tss),
   result({Timestamp, Value}, As, Tss)
;
result({last, Value}, As, [Timestamp |_R]=Tss) ->
   result({Timestamp, Value}, As, Tss)
;
result({Timestamp, Value}, As, _T) when is_integer(Timestamp) ->
   flowdata:set_field(#data_point{ts = Timestamp}, As, Value)
;
result({Tss, ValueList}, As, _Tss) when is_list(Tss) andalso is_list(ValueList)->
   L = lists:zip(Tss, ValueList),
%%   lists:foreach(fun({T, V}) ->
      % lager:notice("out: ~p :: ~p",[faxe_time:to_htime(T), V]) end, L),
   Points = lists:map(fun({_T, _V}=P) -> result(P, As, _T) end, L),
   #data_batch{points = Points}.

