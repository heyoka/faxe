%% Date: 31.03.23 - 09:40
%% execute a statement in a one-shot manner or periodically against a postgreSQL compatible db, (PostGre, Crate, ...)
%% Ⓒ 2023 heyoka
%%
-module(esp_postgre_statement).
-author("Alexander Minichmair").

-include("faxe.hrl").
-include("faxe_epgsql_response.hrl").

-behavior(df_component).
%% API
-export([init/3, process/3, options/0, handle_info/2, check_options/0, shutdown/1, init/4, format_state/1]).

-record(state, {
   host              :: string(),
   port              :: non_neg_integer(),
   user              :: string(),
   pass              :: string(),
   client            :: pid(),
   statement         :: iodata(),
   statement_field   :: binary(),
   retried = 0       :: non_neg_integer(),
   retries = 2       :: non_neg_integer(),
   db_opts           :: map(),
   on_trigger        :: true|false,
   response_def      :: faxe_epgsql_response(),
   interval          :: pos_integer(),
   is_done = false   :: true|false,
   fn_id
}).

-define(DB_OPTIONS, #{
   codecs => [
      {faxe_epgsql_codec, nil},
      {epgsql_codec_json, {jiffy, [], [return_maps]}}],
   timeout => 5000
}).

-define(RETRY_INTERVAL, 700).


options() ->
   [
      {host, string, {crate, host}},
      {port, integer, {crate, port}},
      {tls, boolean, {crate, tls, enable}},
      {user, string, {crate, user}},
      {pass, string, <<>>},
      {statement, string, undefined},
      {statement_field, string, undefined},
      {retries, integer, 2},
      {start_on_trigger, boolean, false},
      {every, duration, undefined},
      {result_type, string, <<"batch">>}
   ].

check_options() ->
   [
      {one_of, result_type, [<<"batch">>, <<"point">>]},
      {one_of_params, [statement, statement_field]}
      ,
      {func, statement_field,
         fun(Value, #{start_on_trigger := Trig}) ->
            case {Value, Trig} of
               {Bin, false} when is_binary(Bin) -> false;
               _ -> true
            end
         end,
         <<" can only be used, when 'start_on_trigger' is true">>}
   ].


format_state(State) ->
   State#state{client = undefined}.


init(NodeId, Ins, Opts, #node_state{state = State=#state{is_done = IsDone}}) ->
   case IsDone of
      true ->
         %% we are done, do nothing, additionally prevent trigger from message (in case)
         lager:notice("init with state, we are done already, so do nothing"),
         {ok, all, State#state{on_trigger = false}};
      false ->
         %% not done, init normally, then
         {ok, Mode, _NewState} = init(NodeId, Ins, Opts),
         {ok, Mode, State}
   end.
init(NodeId, _Inputs, #{host := Host0, port := Port, user := User, pass := Pass,
   statement := Q0, statement_field := StmtField, retries := Retries, tls := Ssl,
   result_type := RType0, start_on_trigger := OnTrigger, every := Every0}) ->

%%   lager:warning("STATEMENT is ~p",[Q0]),
   process_flag(trap_exit, true),
   Host = binary_to_list(Host0),
   Opts = #{
      host => Host, port => Port, username => faxe_util:to_list(User),
      password => faxe_util:to_list(Pass), ssl => Ssl},
   DBOpts = maps:merge(?DB_OPTIONS, Opts),
   RType = erlang:binary_to_existing_atom(RType0),
   Response = faxe_epgsql_response:new(<<"ts">>, RType),
   Every = case faxe_time:is_duration_string(Every0) of true -> faxe_time:duration_to_ms(Every0); _ -> undefined end,

   State = #state{
      host = Host, port = Port, user = User, pass = Pass, db_opts = DBOpts, fn_id = NodeId,
      statement = Q0, statement_field = StmtField, retries = max(1, Retries),
      on_trigger = OnTrigger, response_def = Response, interval = Every},

   connection_registry:reg(NodeId, Host, Port, <<"pgsql">>),
   erlang:send_after(0, self(), reconnect),
   {ok, true, State}.

process(_In, _Item, State = #state{on_trigger = false}) ->
   lager:info("item received, but on_trigger is false"),
   {ok, State};
process(_In, Item, State = #state{on_trigger = true, statement_field = SField, statement = Stmt}) ->
   NewStatement =
   case SField of
      undefined -> Stmt;
      _ when is_binary(SField) ->
         Point =
         case is_record(Item, data_batch) of
            true -> [P|_] = Item#data_batch.points, P;
            false -> Item
         end,
         flowdata:field(Point, SField)
   end,
   send_execute(),
   {ok, State#state{statement = NewStatement}}.

handle_info(execute_statement, State = #state{retries = Tries, retried = Tries}) ->
   %% reached max retries
   NewState =
   case State#state.interval of
      undefined -> close(State);
      Interval ->
         send_execute(Interval),
%%         erlang:send_after(Interval, self(), execute_statement),
         State#state{retried = 0}
   end,
%%   NewState = close(State),
   lager:warning("Could not execute statement with ~p retries!",[Tries]),
   {ok, NewState};
handle_info(execute_statement, State = #state{retried = Tried}) ->
   NewState = execute(State#state{retried = Tried + 1}),
   {ok, NewState};
handle_info({'EXIT', _C, normal}, State = #state{}) ->
   {ok, State#state{client = undefined}};
handle_info({'EXIT', _C, Reason}, State = #state{}) ->
   lager:warning("EXIT epgsql with reason: ~p",[Reason]),
   NewState = connect(State),
   {ok, NewState#state{}};
handle_info(reconnect, State) ->
   {ok, connect(State)};
handle_info(What, State) ->
   lager:warning("++other info : ~p",[What]),
   {ok, State}.

shutdown(State = #state{client = _C}) ->
   close(State).

%% when done, then do nothing
connect(State = #state{is_done = true}) ->
   State;
connect(State = #state{db_opts = Opts}) ->
   connection_registry:connecting(),
   case epgsql:connect(Opts) of
      {ok, C} ->
         connection_registry:connected(),
         NewState = State#state{client = C},
         maybe_send_execute(NewState),
         NewState;
      {error, What} ->
         lager:warning("Error connecting to crate: ~p",[What]),
         State
   end.

maybe_send_execute(#state{on_trigger = true}) ->
   lager:info("WAITING FOR TRIGGER ...."),
   ok;
maybe_send_execute(#state{on_trigger = false}) ->
   send_execute().

send_execute() ->
   send_execute(0).
send_execute(Timeout) ->
   erlang:send_after(Timeout, self(), execute_statement).

execute(State = #state{}) ->
   case do_execute(State) of
      true ->
         %% done emit empty data-point
         dataflow:emit(flowdata:new()),
         %% close connection, we are done here
         close_or_execute(State);
      {true, Item} ->
         %% done emit result data
         dataflow:emit(Item),
         %% close connection, we are done here
         close_or_execute(State);
      false ->
         send_execute(?RETRY_INTERVAL),
         State
   end.

do_execute(_State = #state{client = C, statement = Statement, response_def = ResDef}) ->
   ResponseDef = ResDef#faxe_epgsql_response{default_timestamp = faxe_time:now()},
   Response = epgsql:equery(C, Statement),
%%   lager:info("response from query ~p",[Response]),
   case catch faxe_epgsql_response:handle(Response, ResponseDef) of
      ok ->
         true;
      {ok, Data, _NewResponseDef} ->
%%         lager:notice("got response data: ~p",[Data]),
         {true, Data};
      {error, {error,error,<<"XX000">>,internal_error, ErrorMessage, _Trace}} ->
         lager:error("Error executing Statement: ~p",[ErrorMessage]),
         false;
      {error, What} ->
         lager:error("Error executing Statement: ~p",[What]),
         false;
      What ->
         lager:error("Error handling CRATE response: ~p",[What]),
         false
   end.

close_or_execute(State = #state{interval = undefined}) ->
   close(State);
close_or_execute(State = #state{interval = Interval}) ->
   send_execute(Interval),
   State#state{retried = 0}.

close(State = #state{client = C}) ->
   catch epgsql:close(C),
   connection_registry:disconnected(),
   %% set on_trigger to false, to avoid re-trigger by next incoming item
   State#state{client = undefined, on_trigger = false, is_done = true}.