%% Date: 05.01.17 - 14:11
%% Ⓒ 2019 heyoka
%%
%% @doc
%% Sample a stream of data based on count or duration.
%% if rate is an integer, every n'th message will be passed on to the next node(s)
%% if rate is a duration literal, the first message that comes in after the timeout will be passed on
%% @end
-module(esp_sample).
-author("Alexander Minichmair").

%% API
-behavior(df_component).

-include("faxe.hrl").
%% API
-export([init/3, process/3, options/0, handle_info/2, check_options/0, init/4]).

-record(state, {
   node_id,
   point_count = 1,
   rate_count,
   rate_interval,
   gate_open = false
}).

options() -> [{rate, any}].

check_options() ->
   [{func, rate, fun check_rate/1, <<", must be of type 'integer' or 'duration'">>}].

check_rate(Param) when is_integer(Param) -> true;
check_rate(Param) when is_binary(Param) -> faxe_time:is_duration_string(Param);
check_rate(_) -> false.

init(NodeId, _Ins, #{rate := Rate}) ->
   State = #state{node_id = NodeId},
   NewState =
   case Rate of
      _Int when is_integer(Rate) ->
         State#state{rate_count = Rate};
      _Dur when is_binary(Rate) ->
         Interval = faxe_time:duration_to_ms(Rate),
         State#state{rate_interval = Interval}
   end,
   start_timer(NewState),
   {ok, true, NewState}.

init(NodeId, Ins, Opts, #node_state{state = #{point_count := PCount}}) ->
   {ok, Mode, InitState} = init(NodeId, Ins, Opts),
   {ok, Mode, InitState#state{point_count = PCount}}.


process(_In, Item, State = #state{rate_interval = undefined, rate_count = Count, point_count = Count}) ->
   {emit, Item, State#state{point_count = 1}};
process(_In, _Item, State = #state{rate_interval = undefined, point_count = Count}) ->
   {ok, State#state{point_count = Count+1}};

process(_In, Item, State = #state{gate_open = true}) ->
   start_timer(State),
   {emit, Item, State#state{gate_open = false}};
process(_In, _Item, State) ->
   {ok, State}.

handle_info(open_gate, State) ->
   {ok, State#state{gate_open = true}};
handle_info(_R, State) ->
   {ok, State}.

start_timer(#state{rate_interval = undefined}) ->
   ok;
start_timer(#state{rate_interval = Interval}) ->
   erlang:send_after(Interval, self(), open_gate).