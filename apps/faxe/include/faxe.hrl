-author("Alexander Minichmair").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-include("dataflow.hrl").
-include("timeformats.hrl").
-include("faxe_metrics.hrl").

-define(KEY_BALENA_DEVICE_UUID, "BALENA_DEVICE_UUID").
-define(KEY_FAXE_DEVICE_UUID, "FAXE_HOST").

-record(faxe_timer, {
   last_time = 0 :: non_neg_integer(),
   interval,
   message = poll,
   timer_ref
}).

-record(mem_queue, {
   q,
   max,
   current
}).
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%
%% db records
%%
-record(ids, {
   id, count
}).

-record(faxe_user, {
   name, pw, role = admin
}).

-record(task, {
   id                   :: list()|binary(),
   name                 :: binary(),
   dfs                  :: binary(),
   definition           :: map(),
   date                 :: faxe_time:date(),
   pid                  :: pid(), %% current or last pid of task process
   last_start           :: faxe_time:date(),
   last_stop            :: faxe_time:date(),
   permanent = false    :: true|false,
   is_running = false,
   template_vars = #{}  :: map(), %% list of template vars
   template  = <<>>, %% task is created from this template
   tags = [], %% a list of tags for the task
%%   , concurrency = 1 :: non_neg_integer() %% concurrency for the task
   group = undefined :: undefined|binary(),
   group_leader = false :: true|false
}).

-record(template, {
   id          :: list()|binary(),
   name        :: binary(),
   dfs         :: binary(),
   definition  :: map(),
   date        :: faxe_time:date(),
   vars = []   :: list() %% list of vars that can be overridden
}).

-record(tag_tasks, {
   tag,
   tasks = []
}).
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-record(task_modes, {
   temporary = false    :: true|false,
   temp_ttl = infinity  :: infinity|non_neg_integer(),
   permanent = false    :: true|false,
   run_mode = push      :: push|pull,
   concurrency = 1      :: non_neg_integer()

}).



