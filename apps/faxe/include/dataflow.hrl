%% Date: 28.12.16 - 18:28
%% Ⓒ 2016 Alexander Minichmair
-author("Alexander Minichmair").

-include("df_types.hrl").

-ifdef(debug).
-define(LOG(Msg, Args), io:format(Msg ++ "~n", Args)).
-else.
-define(LOG(Msg, Args), ok).
-endif.

%% df_component state type
-type cbstate()         :: term().

-record(subscription, {
   flow_mode = push,
   publisher_pid,
   publisher_port,
   subscriber_pid,
   subscriber_port,
   out_buffer,
   pending = false
}).


-record(c_state, {
   flow_mode = push     :: push | pull,
   graph_id             :: term(), %% the id of the graph this node belongs to
   node_id              :: term(), %% this nodes id
   component            :: atom(), %% callbacks module name
   cb_state             :: cbstate(), %% state for callback
   cb_handle_info       :: true | false,
   inports              :: list(), %% list of inputs {port, pid}
   subscriptions        :: list(#subscription{}),
   auto_request         :: none | all | emit,
   history              :: list(),
   emitted = 0          :: non_neg_integer(),
   ls_mem,
   ls_mem_field,
   ls_mem_ttl,
   ls_mem_set
}).

-record(data_point, {
   ts                :: non_neg_integer(), %% timestamp in ms
   fields   = #{}    :: map(),
   tags     = #{}    :: map(),
   id       = <<>>   :: binary()
}).

-record(data_batch, {
   id                :: binary(),
   points            :: list(#data_point{}),
   start             :: non_neg_integer(),
   ed                :: non_neg_integer()
}).
