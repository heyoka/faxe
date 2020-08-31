-author("Alexander Minichmair").


%%% NODE METRICS
-define(METRIC_ERRORS,           <<"processing_errors">>). %% keep flow total
-define(METRIC_ITEMS_IN,         <<"items_in">>). %% keep flow total
-define(METRIC_ITEMS_OUT,        <<"items_out">>). %% keep flow total
-define(METRIC_ITEMS_PROCESSED,  <<"items_processed">>). %% flow only, max(items_in)
-define(METRIC_MEM_USED,         <<"mem_used">>). %% calc flow total
-define(METRIC_MSG_Q_SIZE,       <<"msg_q_size">>). %% calc flow total
-define(METRIC_PROCESSING_TIME,  <<"processing_time">>).
-define(METRIC_READING_TIME,     <<"reading_time">>).
-define(METRIC_BYTES_READ,       <<"bytes_read">>). %% keep flow total
-define(METRIC_SENDING_TIME,     <<"sending_time">>).
-define(METRIC_BYTES_SENT,       <<"bytes_sent">>). %% keep flow total

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
   node_id           :: undefined|binary(),
   metric_name       :: binary(),
   name              :: binary(),
   metric_type       :: meter|histogram|counter|gauge|spiral,
   metric_opts       :: undefined|list(),
   desc              :: undefined|binary(),
   node_module       :: atom(),
   node_pid          :: undefined|pid()
}).