-author("Alexander Minichmair").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-include("dataflow.hrl").
-include("timeformats.hrl").

-define(FOLSOM_ERROR_HISTORY, "_processing_errors").

-define(OPT_TYPES, [
   is_set,
   string,
   duration,
   any,
   integer,
   float,
   list,
   atom,
   identifier,
   reference
]).

-record(faxe_timer, {
   last_time = 0 :: non_neg_integer(),
   interval,
   message = poll,
   timer_ref
}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%
%% db records
%%
-record(ids, {
   id, count
}).

-record(task, {
   id :: list()|binary(),
   name :: binary(),
   dfs :: binary(),
   definition :: map(),
   date :: faxe_time:date(),
   pid :: pid(),
   last_start :: faxe_time:date(),
   last_stop :: faxe_time:date(),
   permanent = false :: true|false,
   is_running = false,
   template_vars = [] :: list(), %% list of template vars
   template %% task is created from this template
}).

-record(template, {
   id :: list()|binary(),
   name :: binary(),
   dfs :: binary(),
   definition :: map(),
   date :: faxe_time:date(),
   vars = [] :: list() %% list of vars that can be overridden
}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-record(task_modes, {
   temporary = false :: true|false,
   temp_ttl = infinity :: infinity|non_neg_integer(),
   permanent = false :: true|false,
   run_mode = push :: push|pull

}).