%% Date: 23.07.2021
%% Mongo DB find
%% Ⓒ 2021 heyoka
%%
-module(esp_mongo_query).
-author("Alexander Minichmair").

-include("faxe.hrl").

-behavior(df_component).
%% API
-export([
   init/3, process/3, options/0, handle_info/2,
   metrics/0, shutdown/1, check_options/0]).

-record(state, {
   host :: string(),
   port :: non_neg_integer(),
   selector :: map(),

   user :: string(), %% Schema
   pass :: string(), %%
   database :: iodata(),
   collection :: binary(),
   as :: binary(),
   client,
   client_ref,
   db_opts,
   every,
   align = false,
   timer,
   fn_id
}).

-define(DB_OPTIONS, #{
   timeout => 3000
}).


options() ->
   [
      {host, string},
      {port, integer, 27017},
      {user, string, <<>>},
      {pass, string, <<>>},
      {database, string},
      {collection, string},
      {query, string, <<"{}">>}, %% json string
      {as, binary, undefined},
      {time_field, string, <<"ts">>},
      {every, duration, undefined},
      {align, is_set, false}
   ].

check_options() ->
   [
      {func, query,
         fun(Selector) ->
            case catch(jiffy:decode(Selector, [return_maps])) of
               S when is_map(S) orelse is_list(S) -> true;
               _ -> false
            end
         end,
         <<" seems not to be valid json">>}
   ].

metrics() ->
   [
      {?METRIC_READING_TIME, histogram, [slide, 60], "Network time for sending a message."}
%%      {?METRIC_BYTES_READ, histogram, [slide, 60], "Size of item sent in kib."}
   ].

init(NodeId, _Inputs, #{host := Host0, port := Port, user := User, every := Every, as := As,
      pass := Pass, query := JsonString, align := Align, database := DB, collection := Collection}) ->

   %% we need to trap exists form the result cursors
   process_flag(trap_exit, true),
   Host = binary_to_list(Host0),

   Query = jiffy:decode(JsonString, [return_maps]),

   DBOpts = [{host, Host}, {port, Port}, {login, User}, {password, Pass}, {database, DB}],
   connection_registry:reg(NodeId, Host, Port, <<"mongodb">>),
   State = #state{host = Host, port = Port, user = User, pass = Pass, selector = Query, database = DB,
      db_opts = DBOpts, every = Every, align = Align, fn_id = NodeId, collection = Collection, as = As},
   erlang:send_after(0, self(), reconnect),
   {ok, all, State}.

%% read on incoming data-items
process(_In, _DataItem, State = #state{}) ->
   handle_info(query, State).

handle_info(reconnect, State = #state{client = Client}) ->
   NewState =
   case Client /= undefined andalso is_process_alive(Client) of
      true -> State;
      false -> connect(State)
   end,
   {ok, NewState};
handle_info(query, State = #state{timer = Timer, client = C, collection = Coll, selector = Sel, database = DB}) ->
   NewTimer = faxe_time:timer_next(Timer),
   %% do query
   Res = (catch mc_worker_api:find(C, Coll, Sel)),
   node_metrics:metric(?METRIC_ITEMS_IN, 1, State#state.fn_id),
   case handle_response(Res, State) of
      {ok, DataPoints} ->
         {emit, {1, #data_batch{points = DataPoints}}, State#state{timer = NewTimer}};
      {error, empty_response} ->
         lager:notice("Empty response with query: ~p on collection: ~p in database: ~p", [Sel, Coll, DB]),
         {ok, State#state{timer = NewTimer}};
      {error, Reason} ->
         lager:notice("Error with mongodb response: ~p",[Reason]),
         {ok, State#state{timer = NewTimer}}
   end;

handle_info({'DOWN', _MonitorRef, process, Pid, _Info}, State=#state{client = Pid, timer = Timer}) ->
   connection_registry:disconnected(),
   lager:notice("mongodb connection DOWN"),
   NewTimer = cancel_timer(Timer),
   erlang:send_after(1000, self(), reconnect),
   {ok, State#state{timer = NewTimer}};
handle_info(What, State) ->
   lager:debug("++other info : ~p",[What]),
   {ok, State}.


shutdown(#state{client = Client}) ->
   catch mc_worker_api:disconnect(Client).

-spec connect(#state{}) -> #state{}.
connect(State = #state{db_opts = Opts}) ->
   connection_registry:connecting(),
   case mc_worker_api:connect(Opts) of
      {ok, C} ->
         erlang:monitor(process, C),
         connection_registry:connected(),
         init_timer(State#state{client = C});
      {error, _What} ->
         lager:notice("failed to connect to mongodb with Reason: ~p",[_What]),
         State
   end.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
handle_response({ok, Cursor}, State) when is_pid(Cursor) ->
   Rows = mc_cursor:rest(Cursor),
   mc_cursor:close(Cursor),
   lager:info("Rows: ~p",[Rows]),
   build(Rows, State);
handle_response([], _State) ->
   {error, empty_response};
handle_response({error, _Reason} = E, _State) ->
   E.

build(Rows, State) ->
   build(Rows, [], State).
build([], Points, _S) ->
   {ok, Points};
build([Row|Rows], Points, S=#state{timer = Timer, as = As}) ->
   Ts = Timer#faxe_timer.last_time,
   P0 = #data_point{ts = Ts, fields = maps:without([<<"_id">>], Row)},
   NewPoint =
   case As of
      undefined -> P0;
      Bin when is_binary(Bin) -> flowdata:set_root(P0, As)
   end,
   build(Rows, Points ++ [NewPoint], S).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
init_timer(S = #state{align = Align, every = Every}) ->
   Timer = faxe_time:init_timer(Align, Every, query),
   S#state{timer = Timer}.

-spec cancel_timer(#faxe_timer{}|undefined) -> #faxe_timer{}|undefined.
cancel_timer(Timer) ->
   case catch (faxe_time:timer_cancel(Timer)) of
      T = #faxe_timer{} -> T;
      _ -> Timer
   end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% TESTS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-ifdef(TEST).
-endif.