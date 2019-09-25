%% Ⓒ 2019 heyoka
%%
%% @doc
%% The time_diff node adds a field to the current data-item
%% containing the difference between the timestamps of the consecutive items
%% To make it clear, this node will measure the arrival time difference of consecutive data-items.
%%
%% With the 'as' option, the name of the output field can be changed.
%%
%% 'as' defaults to "elapsed"
%%
%% unit for output value is milliseconds
%% @end
-module(esp_time_diff).
-author("Alexander Minichmair").

%% API
-behavior(df_component).

-include("faxe.hrl").
%% API
-export([init/3, process/3, options/0]).

-record(state, {
   node_id,
   as,
   last_time
}).

options() -> [{as, binary, <<"elapsed">>}].

init(NodeId, _Ins, #{as := As}) ->
   {ok, all, #state{node_id = NodeId, as = As}}.

process(_In, _Item, State = #state{last_time = undefined}) ->
   {ok, State#state{last_time = faxe_time:now()}};
process(_In, Item, State = #state{as = As, last_time = Last}) ->
   Now = faxe_time:now(),
   NewItem = flowdata:set_field(Item, As, Now - Last),
   {emit, NewItem, State#state{last_time = Now}}.


%%%%%%%%%%%%
-ifdef(TEST).
-endif.