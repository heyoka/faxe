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
   graph_id             :: binary(), %% the id of the graph this node belongs to
   node_id              :: binary(), %% this nodes id
   node_index           :: {binary(), binary()}, %% index for subscriptions
   flow_node_id         :: binary(), %% combination of graph_id and node_id
   component            :: atom(), %% callbacks module name
   cb_state             :: cbstate(), %% state for callback
   cb_handle_info       :: true | false,
   cb_inited = false    :: true | false,
   inports              :: list(), %% list of inputs {port, pid}
   auto_request         :: none | all | emit,
   emitted = 0          :: non_neg_integer(),
   emit_debug = false   :: true | false
}).

-record(data_point, {
   ts                :: non_neg_integer(), %% timestamp in ms
   fields   = #{}    :: map(),
   tags     = #{}    :: map(),
   id       = <<>>   :: binary()
}).

-record(data_batch, {
   id                :: binary(),
   points   = []     :: list(#data_point{}),
   start             :: non_neg_integer(),
   ed                :: non_neg_integer()
}).
