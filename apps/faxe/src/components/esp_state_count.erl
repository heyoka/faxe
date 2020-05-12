%% Date: 15.07.2019 - 09:55
%% Ⓒ 2019 heyoka
%% @doc
%% Computes the number of consecutive points in a given state. The state is defined via a lambda expression.
%% For each consecutive point for which the expression evaluates as true,
%% the state count will be incremented.
%% When a point evaluates to false, the state count is reset.
%%
%% The state count will be added as an additional int64 field to each point.
%% If the expression evaluates to false, the value will be -1.
%% If the expression generates an error during evaluation, the point is discarded
%% and does not affect the state count.
%%
-module(esp_state_count).
-author("Alexander Minichmair").

%% API
-behavior(df_component).

-include("faxe.hrl").
%% API
-export([init/3, process/3, options/0]).

-record(state, {
   node_id,
   lambda,
   as,
   last_count = 0,
   state_change

}).

options() -> [
   {lambda, lambda},
   {as, binary, <<"state_count">>}
].

init(_NodeId, _Ins, #{lambda := Lambda, as := As}) ->
   StateTrack = state_change:new(Lambda),
   {ok, all, #state{lambda = Lambda, as = As, state_change = StateTrack}}.

process(_In, #data_batch{points = _Points} = _Batch, _State = #state{lambda = _Lambda}) ->
   {error, not_implemented};
process(_Inport, #data_point{} = Point, State = #state{state_change = StateTrack, as = As}) ->
   case state_change:process(StateTrack, Point) of
      {ok, NewStateTrack} ->
         Count = state_change:get_count(NewStateTrack),
         NewPoint = flowdata:set_field(Point, As, Count),
         {emit, NewPoint, State#state{state_change = NewStateTrack}};
      {error, _Error} ->
         {ok, State}
   end.

