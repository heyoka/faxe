%% Date: 08.20.21 - 08:57
%% Ⓒ 2021 heyoka
%%
%% @doc
%%
%% @end
%% @todo persist collection to disc and decided how to deal with a restart of the node
%% @todo implement 'as', 'max_age', 'keep_as'
%% @todo maybe make this node handle multiple collections ?
%%
-module(esp_collect).
-author("Alexander Minichmair").


%% API
-behavior(df_component).

-include("faxe.hrl").
%% API
-export([init/3, process/3, options/0
%%   , check_options/0
   , wants/0, emits/0, handle_info/2, check_options/0, do_process/2]).

-define(UPDATE_NEVER, <<"never">>).
-define(UPDATE_ALWAYS, <<"always">>).

-define(TYPE_SET, <<"set">>).
-define(TYPE_LIST, <<"list">>).

-define(TAG_ADDED, <<"added">>).
-define(TAG_REMOVED, <<"removed">>).

-record(state, {
   node_id,
   buffer = undefined, %% holds a proplist with {key, point} -> current collection buffer
   removed_buffer = undefined, %% holds a proplist with {key, point} of points that were removed since the last emit

   add_function,
   remove_function,
   update,
   update_state_fun,
   emit_interval,
   fields
   ,keep
   ,keep_as,
   type = ?TYPE_SET,
   as,
   max_age
}).

options() -> [
   {key_fields, string_list},
   {type, string, ?TYPE_SET},
   {add, lambda},
   {remove, lambda},
   {update, any, ?UPDATE_NEVER}, %% 'never', 'always' or lambda expression
   {update_state, lambda, undefined}, %% 'never', 'always' or lambda expression
   {emit_every, duration, undefined},
   {keep, string_list, []}, %% a list of field path to keep for every data_point
   {keep_as, string_list, []}, %% rename the kept fields
   {as, string, <<"collected">>}, %% rename the whole field construct on output
   {max_age, duration, undefined}
].

check_options() ->
   [
      {same_length, [keep, keep_as]},
      {func, update,
         fun(Val) ->
            is_function(Val) orelse Val == ?UPDATE_NEVER orelse Val == ?UPDATE_ALWAYS
         end,
         <<" can only be 'never', 'always' or a lambda expression">>},
      {one_of, type, [?TYPE_SET, ?TYPE_LIST]}
   ].

wants() -> point.
emits() -> batch.

init(NodeId, _Ins, #{key_fields := Fields, add := AddFunc, remove := RemFunc, update := Update, type := Type,
   emit_every := EmitEvery, keep := Keep, keep_as := KeepAs, as := As, max_age := MaxAge0, update_state := UpStateFun}) ->

   EmitInterval = case EmitEvery of undefined -> undefined; _ -> faxe_time:duration_to_ms(EmitEvery) end,
   MaxAge = case MaxAge0 of undefined -> undefined; Age -> faxe_time:duration_to_ms(Age) end,
   Aliases = case KeepAs of [] -> Keep; _ -> KeepAs end,
   {ok, all,
      #state{
         node_id = NodeId,
         fields = Fields,
         type = Type,
         add_function = AddFunc,
         remove_function = RemFunc,
         update = Update,
         update_state_fun = UpStateFun,
         emit_interval = EmitInterval,
         keep = Keep,
         keep_as = Aliases,
         as = As,
         max_age = MaxAge}}.

process(Port, #data_point{} = Point, State = #state{buffer = undefined}) ->
   maybe_start_emit_timeout(State),
   process(Port, Point, State#state{buffer = []});
process(_Port, #data_point{} = Point, State = #state{fields = _Field}) ->
   {T, Res} = timer:tc(?MODULE, do_process, [Point, State]),
%%   lager:info("Took: ~p my",[T]),
   case Res of
      {ok, State} -> {ok, State};
      {Changed, NewState} ->
         maybe_emit(Changed, NewState)
%%            do_emit(State#state{buffer = NewBuffer})
   end;
process(Port, Batch = #data_batch{}, State = #state{buffer = undefined}) ->
   process(Port, Batch, State#state{buffer = []});
process(_Port, #data_batch{points = Points}, State) ->

   ProcessFun =
   fun(Point, {Changed0, State0}) ->
      {Changed1, NewState} = do_process(Point, State0),
      ChangedNew = (Changed0 == true orelse Changed1 == true),
      {ChangedNew, NewState}
   end,
   {T, {Changed, ResState}} = timer:tc(lists, foldl, [ProcessFun, {false, State}, Points]),
   lager:notice("it took ~p my to process ~p points (changed: ~p)",[T, length(Points), Changed]),
   maybe_emit(Changed, ResState).

%%do_process(#data_point{} = Point, State = #state{buffer = Buffer}) ->
%%   case keyval(Point, State) of
%%      undefined -> {ok, State};
%%      KeyVal ->
%%         case proplists:get_value(KeyVal, Buffer) of
%%            undefined ->
%%               %% it is not there so only adding will be appropiate
%%               maybe_add(Point, KeyVal, State);
%%            _ ->
%%               %% entry with this key is present, so update or remove possible
%%               %% if update did happen, we do not bother to test if remove should be done
%%%%               lager:notice("maybe_update_state for: ~p",[KeyVal]),
%%               {ChangedBool, NewState} = maybe_update_state(Point, KeyVal, State),
%%               case ChangedBool of
%%                  true -> {true, NewState};
%%                  false -> maybe_remove(Point, KeyVal, NewState)
%%               end
%%         end
%%   end.

do_process(#data_point{} = Point, State = #state{buffer = Buffer}) ->
   case is_in(Point, State) of
      {undefined, _} -> {ok, State};
      {false, KeyVal} -> maybe_add(Point, KeyVal, State);
      {true, KeyVal} ->
         {ChangedBool, NewState} = maybe_update_state(Point, KeyVal, State),
         case ChangedBool of
            true -> {true, NewState};
            false -> maybe_remove(Point, KeyVal, NewState)
         end
   end.

-spec is_in(#data_point{}, #state{}) -> {undefined|true|false, KeyVal :: term()}.
is_in(#data_point{} = Point, State = #state{buffer = Buffer}) ->
   KeyVal = keyval(Point, State),
   R =
   case KeyVal of
      undefined -> undefined;
      _ ->
         %% get the point
         case proplists:get_value(KeyVal, Buffer) of
            undefined -> false;
            P ->
               flowdata:field(P, ?TAG_REMOVED) /= true
         end
   end,
%%   lager:notice("keyval ~p", [KeyVal]),
   {R, KeyVal}.

handle_info(emit_timeout, State = #state{}) ->
   maybe_start_emit_timeout(State),
   do_emit(State).

keyval(Point, #state{fields = [Field]}) when is_binary(Field) ->
   flowdata:field(Point, Field);
keyval(Point, #state{fields = Fields}) ->
   FVals = flowdata:fields(Point, Fields),
   case lists:all(fun(Val) -> Val == undefined end, FVals) of
      true -> undefined;
      false -> term_to_binary(FVals)
   end.


maybe_add(Point = #data_point{ts = _Ts}, KeyVal, State = #state{add_function = AddFunc}) ->
   case catch(faxe_lambda:execute(Point, AddFunc)) of
      true ->
%%         lager:info("add: ~p",[KeyVal]),
         add(KeyVal, Point, State);
      _ -> {false, State}
   end.

maybe_remove(Point = #data_point{ts = _Ts}, KeyVal, State = #state{remove_function = RemFunc}) ->
   case catch(faxe_lambda:execute(Point, RemFunc)) of
      true -> {true, mark_removed(KeyVal, State)};
      _ -> {false, State}
   end.

%%maybe_update(_Point, _KeyVal, S = #state{update = ?UPDATE_NEVER, buffer = Buffer}) ->
%%   {false, S};
%%maybe_update(Point, KeyVal, State = #state{}) ->
%%   update(KeyVal, Point, State).

maybe_update_state(_Point, _KeyVal, S = #state{update_state_fun = undefined}) ->
   {false, S};
maybe_update_state(Point=#data_point{fields = Fields}, KeyVal, State = #state{update_state_fun = Fun, buffer = Buffer}) ->
   StatePoint = proplists:get_value(KeyVal, Buffer),
   FunPoint = Point#data_point{fields = Fields#{<<"__state">> => StatePoint#data_point.fields}},
%%      flowdata:set_field(Point, <<"__state">>, StatePoint#data_point.fields),
%%   lager:notice("FunPoint is: ~p",[FunPoint]),
   case catch(faxe_lambda:execute(FunPoint, Fun)) of
      true -> {true, replace(KeyVal, Point, State)};
      _ -> {false, Buffer}
   end.



maybe_emit(true, State = #state{emit_interval = undefined}) ->
   do_emit(State);
maybe_emit(_, State = #state{}) ->
   {ok, State}.

do_emit(State = #state{buffer = Buff}) ->
   lager:notice("buffer length before emit: ~p", [length(Buff)]),
   CFun =
   fun({_, P}, {Added, Kept, Removed}) ->
      case flowdata:field(P, ?TAG_ADDED) of
         1 -> {Added+1, Kept, Removed};
         _ ->
            case flowdata:field(P, ?TAG_REMOVED) of
               1 -> {Added, Kept, Removed+1};
               _ -> {Added, Kept+1, Removed}
            end
      end
   end,
   {A, K, R} = lists:foldl(CFun, {0,0,0}, Buff),
   lager:notice("Added: ~p, Kept: ~p, Removed: ~p",[A, K, R]),
   Points0 = [keep(Val, State) || {_Key, Val} <- Buff],
%%   lager:notice("emit: ~p", [[Key || {Key, _Val} <- Buff]]),
   %% sort elements by timestamp for data_batch
   Points = lists:keysort(2, Points0),
   Batch = #data_batch{points = Points},
   %% now cleanup removed and added tags
   CleanFun =
   fun({Key, Point}, Acc) ->
      Point0 = flowdata:delete_field(Point, ?TAG_ADDED),
      case flowdata:field(Point0, ?TAG_REMOVED) of
         1 -> proplists:delete(Key, Acc);
         _ -> [{Key, Point0}|proplists:delete(Key, Acc)]
      end
   end,
   CleanedBuffer = lists:foldl(CleanFun, Buff, Buff),
   lager:notice("buffer length after emit: ~p",[length(CleanedBuffer)]),
   {emit, Batch, State#state{buffer = CleanedBuffer}}.

maybe_start_emit_timeout(#state{emit_interval = undefined}) ->
   ok;
maybe_start_emit_timeout(#state{emit_interval = Intv}) ->
   erlang:send_after(Intv, self(), emit_timeout).

keep(DataPoint, #state{keep = []}) ->
   DataPoint;
keep(DataPoint, #state{keep = FieldNames, keep_as = Aliases}) when is_list(FieldNames) ->
%%   lager:notice("keep from: ~p", [DataPoint]),
   Fields = flowdata:fields(DataPoint, FieldNames++[?TAG_REMOVED, ?TAG_ADDED]),
%%   lager:notice("want tp keep: ~p :: ~p as ~p",[FieldNames, Fields, Aliases]),
   Point0 = DataPoint#data_point{fields = #{}, tags = #{}},
   Zipped = lists:zip(Aliases++[?TAG_REMOVED, ?TAG_ADDED], Fields),
   SetFields = [{K, V} || {K, V} <- Zipped, V /= undefined],
   NewPoint0 = flowdata:set_fields(Point0, SetFields),
   NewPoint0.

add(Key, Point, S = #state{buffer = Buffer0, type = ?TYPE_SET}) ->
   case proplists:get_value(Key, Buffer0) of
      undefined ->
%%         lager:warning("add: ~p", [Key]),
         case flowdata:field(Point, ?TAG_ADDED) of
            1 -> lager:info("mark added already when marking remove: ~p",[Key]);
            _ -> ok
         end,
         case flowdata:field(Point, ?TAG_REMOVED) of
            1 -> lager:info("mark removed already when marking added: ~p",[Key]);
            _ -> ok
         end,
         {true, S#state{buffer = [{Key, flowdata:set_field(Point, ?TAG_ADDED, 1)} | Buffer0]}};
      _ ->
%%         lager:info("already exists: ~p", [Key]),
         {false, S}
   end;
add(Key, Point, S = #state{buffer = Buffer0, type = ?TYPE_LIST}) ->
%%   lager:warning("add: ~p", [Key]),
   {true, S#state{buffer = [{Key, Point} | Buffer0]}}.

mark_removed(Key, S = #state{buffer = Buffer0}) ->
   RPoint = proplists:get_value(Key, Buffer0),
   case flowdata:field(RPoint, ?TAG_ADDED) of
      1 -> lager:info("mark added already when marking remove: ~p",[Key]);
      _ -> ok
   end,
   case flowdata:field(RPoint, ?TAG_REMOVED) of
      1 -> lager:info("mark removed already when marking remove: ~p",[Key]);
      _ -> ok
   end,
%%   lager:info("mark removed: ~p",[Key]),
   replace(Key, flowdata:set_field(flowdata:delete_field(RPoint, ?TAG_ADDED), ?TAG_REMOVED, 1), S).

remove(Key, S = #state{buffer = Buffer0, removed_buffer = RBuffer}) ->
   RPoint = proplists:get_value(Key, Buffer0),
%%   lager:warning("remove: ~p", [Key]),
   S#state{buffer = proplists:delete(Key, Buffer0), removed_buffer = [{Key, RPoint}|RBuffer]}.

%%update(Key, Point, State = #state{update = Fun, buffer = Buffer}) when is_function(Fun) ->
%%   case catch(faxe_lambda:execute(Point, Fun)) of
%%      true -> replace(Key, Point, State);
%%      _ -> {false, Buffer}
%%   end;
%%update(Key, Point, S = #state{update = ?UPDATE_ALWAYS}) ->
%%   replace(Key, Point, S).

replace(Key, Point, State = #state{buffer = Buffer}) ->
%%   lager:warning("update: ~p",[Key]),
   State#state{buffer = [{Key, Point}|proplists:delete(Key, Buffer)]}.

%%%%%%%%%%%%%%%%%% TESTS %%%%%%%%%%%%%%%%%%%%%%%%%%
-ifdef(TEST).

state() ->
   RemoveFun = lambda_tests:lambda_helper("Data_mode == 0", ["data_mode"]),
   AddFun = lambda_tests:lambda_helper("Data_mode > 0", ["data_mode"]),
   #state{fields = [<<"data_code_id">>], add_function = AddFun, remove_function = RemoveFun,
      update = ?UPDATE_NEVER, buffer = []}.

point(Idx, Mode) ->
   #data_point{ts = Idx, fields = #{<<"data_code_id">> => Idx, <<"data_mode">> => Mode} }.

%%basic_test() ->
%%   State = state(),
%%   {_Changed1, State1} = R1 = do_process(point(1,1), State),
%%%%   State1 = State#state{buffer = Buffer1},
%%   ?assertEqual({true, [{1, point(1, 1)}]}, R1)
%%   ,
%%   {_Changed2, Buffer2} = R2 = do_process(point(1,1), State1),
%%   State2 = State#state{buffer = Buffer2},
%%   ?assertEqual({false, [{1, point(1, 1)}]}, R2)
%%   ,
%%   {_, Buffer3} = R3 = do_process(point(2,0), State2),
%%   State3 = State#state{buffer = Buffer3},
%%   ?assertEqual({false, [{1, point(1, 1)}]}, R3)
%%   ,
%%   R4 = do_process(point(1,0), State3),
%%   ?assertEqual({true, []}, R4)
%%.



-endif.