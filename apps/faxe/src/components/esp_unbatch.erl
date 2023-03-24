%% Date: 16.02.2020
%% Ⓒ 2020 heyoka
%% @doc
%% This node is used to batch a number(size) of points. As soon as the node has collected size points it will emit them
%% in a data_batch.
%% A timeout can be set, after which all points currently in the batch
%% will be emitted, regardless of the number of collected points.
%% The timeout is started on the first datapoint coming in to an empty batch.
%%
%%
%%
-module(esp_unbatch).
-author("Alexander Minichmair").

-behaviour(df_component).

-include("faxe.hrl").

%% API
-export([init/3, process/3, handle_info/2, options/0, wants/0, emits/0, shutdown/1]).

-record(state, {

}).

options() ->
   [].

wants() -> both.
emits() -> point.

init(_NodeId, _Inputs, #{}) ->
   {ok, false, #state{}}.


process(_, #data_point{} = Point, State=#state{} ) ->
   {emit, Point, State};
process(_, #data_batch{points = Points}, State=#state{} ) ->
   [dataflow:emit(Point) || Point <- Points],
   {ok, State}.

handle_info(_Request, State) ->
   {ok, State}.

shutdown(#state{}) ->
   ok.

%%%===================================================================
%%% Internal functions
%%%===================================================================
