-author("Alexander Minichmair").


%%% NODE METRICS
-define(METRIC_ERRORS,           <<"_processing_errors">>).
-define(METRIC_ITEMS_IN,         <<"_items_in">>).
-define(METRIC_ITEMS_OUT,        <<"_items_out">>).
-define(METRIC_ITEMS_PROCESSED,  <<"_items_processed">>).
-define(METRIC_MEM_USED,         <<"_mem_used">>).
-define(METRIC_MSG_Q_SIZE,       <<"_msg_q_size">>).
-define(METRIC_PROCESSING_TIME,  <<"_processing_time">>).
-define(METRIC_READING_TIME,     <<"_reading_time">>).
-define(METRIC_BYTES_READ,       <<"_bytes_read">>).
-define(METRIC_SENDING_TIME,     <<"_sending_time">>).
-define(METRIC_BYTES_SENT,       <<"_bytes_sent">>).

%% metrics common to all nodes
-define(NODE_COMMON_METRICS, [
   {?METRIC_ITEMS_IN, meter, [], "Number of items received."},
   {?METRIC_ITEMS_OUT, meter, [], "Number of items emitted."},
   {?METRIC_MEM_USED, gauge, [], "Memory usage in kib."},
%%   {?METRIC_MEM_USED, histogram, [slide, 30], "Memory usage in kib."},
   {?METRIC_MSG_Q_SIZE, gauge, [], "Size of process message-queue."},
%%   {?METRIC_MSG_Q_SIZE, histogram, [slide, 30], "Size of process message-queue."},
   {?METRIC_PROCESSING_TIME, histogram, [slide, 30], "The number of milliseconds it took to process (and pot. emit) 1 item."},
   {?METRIC_ERRORS, counter, [], "Number of errors during processing."}
]).

%%%%%%%%%%%%%%%%%%%%%%% METRIC %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-record(metric, {
   flow_id           :: binary(),
   node_id           :: undfined|binary(),
   metric_name       :: binary(),
   name              :: binary(),
   metric_type       :: meter|histogram|counter|gauge|spiral,
   metric_opts       :: undefined|list(),
   desc              :: undefined|binary(),
   node_module       :: atom(),
   node_pid          :: undefined|pid()
}).