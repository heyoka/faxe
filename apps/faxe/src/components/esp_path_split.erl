%% Date: 14.03.24 - 08:11
%% Ⓒ 2024 heyoka
%% @doc
%% Split a data-point into several data-points by the root path element
%% @end
-module(esp_path_split).
-author("Alexander Minichmair").

%% API
-behavior(df_component).

-include("faxe.hrl").
%% API
-export([init/3, process/3, options/0]).

-record(state, {
   path_level,
   include_level,
   include_as
}).

options() -> [
   {include_name, boolean, true},
   {include_as, string, <<"name">>}
].

init(_NodeId, _Ins, #{include_name := Include, include_as := As}) ->
   {ok, all,
      #state{include_level = Include, include_as = As}}.

process(_In, P = #data_point{}, State = #state{}) ->
   do_process(P, State),
   {ok, State};
process(_In, #data_batch{points = Points}, State = #state{}) ->
   Fun = fun(Point) -> do_process(Point, State) end,
   lists:foreach(Fun, Points),
   {ok, State}.

do_process(P = #data_point{fields = Fields}, #state{include_level = Include, include_as = As}) ->
   maps:foreach(
      fun
         (Key, Val) when is_map(Val) ->
            NewPoint0 = P#data_point{fields = Val},
            NewPoint =
               case Include of
                  true -> flowdata:set_field(NewPoint0, As, Key);
                  false -> NewPoint0
               end,
            dataflow:emit(NewPoint);
         (_, _) ->
            ok
      end,
      Fields).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

-endif.