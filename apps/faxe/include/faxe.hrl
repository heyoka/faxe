-author("Alexander Minichmair").

%%-include("amqp_client.hrl").
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-include("dataflow.hrl").
-include("timeformats.hrl").

-export_type([window_state/0, window_events/0]).

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


-record(rule_config, {
   rule_key          :: binary(),
   inputs            :: [binary()],
   output            :: binary(),
   function          :: binary(),
   triggers          :: [binary()]|undefined,
   lastval_max_age   :: undefined | non_neg_integer() | [{binary(), non_neg_integer()}],
   setup_mode        :: undefined | binary()
}).


%% window events is a three tuple of lists : { [timestamps], [values], [whole_events] }
-type window_events() :: {list(), list(), list()}.

-record(esp_win_stats, {
   mark        = 0            :: non_neg_integer(), %% timestamp of oldest event present in the window
   at          = 0            :: non_neg_integer(), %% timestamp of last event, that arrived at the window
   count       = 0            :: non_neg_integer(), %% event-counter
   events      = {[],[],[]}   :: window_events() %% list(s) of all events currently present in the window
}).

-type window_state() :: #esp_win_stats{}.

-record(esp_window, {
   period      = 30*1000               :: non_neg_integer(), %% length of the window in ms or num of events// 30m default
   every       = 15*1000               :: non_neg_integer(), %% emit every ms or num of events // 15m default
   tick_time   = 500                   :: non_neg_integer(), %% clock interval in ms // 500ms by default
   agg_mod     = esp_agg_noop_values   :: atom(), %% module-name of stats
   agg                                 :: any(), % state for agg-mod
   stats       = #esp_win_stats{}      :: #esp_win_stats{}, %% stats state
   ts_field    = <<"ts">>              :: any(), %% field where timestamp lives in the event
   agg_fields  = <<"val">>             :: any(), %% field where values lives, that get computed by agg
   log         = []                    :: list(any()), %% window-log
   align                               :: false | faxe_time:duration()
}).


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
   is_running = false
}).

-record(template, {
   id :: list()|binary(),
   name :: binary(),
   dfs :: binary(),
   definition :: map(),
   date :: faxe_time:date()
}).