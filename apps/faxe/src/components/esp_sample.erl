%% Date: 05.01.17 - 14:11
%% Ⓒ 2019 heyoka
%%
%% @doc
%% Sample a stream of data based on count or duration.
%% @end
-module(esp_sample).
-author("Alexander Minichmair").

%% API
-behavior(df_component).

-include("faxe.hrl").
%% API
-export([init/3, process/3, options/0, handle_info/2]).

-record(state, {
   node_id,
   point_count = 0,
   rate_count,
   rate_interval,
   tolerance
}).

options() -> [{rate, any}, {tolerance, duration, <<"1s">>}].

init(NodeId, _Ins, #{rate := Rate, tolerance := Tol}) ->
   State = #state{node_id = NodeId},
   NewState =
   case Rate of
      _Int when is_integer(Rate) ->
         State#state{rate_count = Rate};
      _Dur when is_binary(Rate) ->
         Tolerance = faxe_time:duration_to_ms(Tol),
         Interval = faxe_time:duration_to_ms(Rate),
         State#state{rate_interval = Interval, tolerance = round(Tolerance / 2)}
   end,
   {ok, all, NewState}.


process(_In, Item, State = #state{rate_interval = undefined, rate_count = Count, point_count = Count}) ->
   {emit, Item, State#state{point_count = 0}};
process(_In, _Item, State = #state{rate_interval = undefined, point_count = Count}) ->
   {ok, State#state{point_count = Count+1}}.

%%process()


handle_info(tolerance_start, State) ->
   {ok, State};
handle_info(tolerance_stop, State) ->
   {ok, State}.