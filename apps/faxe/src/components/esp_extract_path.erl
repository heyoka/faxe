%% Date: 30.12.16 - 23:01
%% Ⓒ 2019 heyoka
%%
%% searches the given paths in a data-point and emits a new point with the values found
%% if a path is not found the 'default' value is used
%% @todo implement for data_batch records
%%
-module(esp_extract_path).
-author("Alexander Minichmair").

-include("dataflow.hrl").

-behavior(df_component).
%% API
-export([init/3, process/3, shutdown/1, options/0, extract/4]).

-record(state, {
   nodeid,
   paths = [],
   as = [],
   default
}).

options() ->
   [{path, binary_list}, {as, binary_list}, {default, any, 0}].

init(NodeId, _Inputs, #{path := Paths, as := As, default := Def} = _Args) ->
   {ok, all, #state{paths = Paths, as = As, default = Def, nodeid = NodeId}}.

process(_Inport, P = #data_point{}, State=#state{paths = Paths, as = As, default = Def}) ->
   lager:notice("~p process [at ~p] , ~p",[State, faxe_time:now(),  {_Inport, P}]),
   {T, NewVal} = timer:tc(?MODULE, extract, [P, Paths, As, Def]),
   lager:info("needed: ~p micro", [T]),
%%   NewVal = extract(P, Paths, As, Def),
   {emit, NewVal, State}.

shutdown(_State) ->
   ok.

extract(Point = #data_point{ts = Ts}, Paths, As, Default) ->
   {_Ix, NewPoint} =
      lists:foldl(
         fun(Pa, {Idx, AccPoint}) ->
            NewV =
               case flowdata:field(Point, Pa) of
                  undefined -> Default;
                  Val -> Val
               end,
            {Idx+1, flowdata:set_field(AccPoint, lists:nth(Idx, As), NewV)}
         end,
         {1, #data_point{ts = Ts}},
         Paths),

   NewPoint.