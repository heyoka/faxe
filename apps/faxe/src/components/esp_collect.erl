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

-record(state, {
   node_id,
   buffer = undefined,
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
   {keep, string_list, undefined}, %% a list of field path to keep for every data_point
   {keep_as, string_list, undefined}, %% rename the kept fields
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
   Aliases = case KeepAs of undefined -> Keep; _ -> KeepAs end,
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
   lager:info("Took: ~p my",[T]),
   case Res of
      {ok, State} -> {ok, State};
      {Changed, NewBuffer} ->
         maybe_emit(Changed, State#state{buffer = NewBuffer})
%%            do_emit(State#state{buffer = NewBuffer})
   end.

do_process(#data_point{} = Point, State = #state{buffer = Buffer}) ->
   case keyval(Point, State) of
      undefined -> {ok, State};
      KeyVal ->
         case proplists:get_value(KeyVal, Buffer) of
            undefined ->
               %% it is not there so only adding will be appropiate
               maybe_add(Point, KeyVal, State);
            _ ->
               %% entry with this key is present, so update or remove possible
               %% if update did happen, we do not bother to test if remove should be done
%%               lager:notice("maybe_update_state for: ~p",[KeyVal]),
               {ChangedBool, NewBuffer0} = maybe_update_state(Point, KeyVal, State),
               case ChangedBool of
                  true -> {true, NewBuffer0};
                  false -> maybe_remove(Point, KeyVal, State#state{buffer = NewBuffer0})
               end
         end
   end.

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


maybe_add(Point = #data_point{ts = _Ts}, KeyVal, State = #state{add_function = AddFunc, buffer = Buffer}) ->
   case catch(faxe_lambda:execute(Point, AddFunc)) of
      true -> add(KeyVal, Point, State);
      _ -> {false, Buffer}
   end.

maybe_remove(Point = #data_point{ts = _Ts}, KeyVal, State = #state{remove_function = RemFunc, buffer = Buffer}) ->
   case catch(faxe_lambda:execute(Point, RemFunc)) of
      true -> {true, remove(KeyVal, State)};
      _ -> {false, Buffer}
   end.

maybe_update(_Point, _KeyVal, #state{update = ?UPDATE_NEVER, buffer = Buffer}) ->
   {false, Buffer};
maybe_update(Point, KeyVal, State = #state{}) ->
   update(KeyVal, Point, State).

maybe_update_state(_Point, _KeyVal, #state{update_state_fun = undefined, buffer = Buffer}) ->
   {false, Buffer};
maybe_update_state(Point=#data_point{fields = Fields}, KeyVal, State = #state{update_state_fun = Fun, buffer = Buffer}) ->
   StatePoint = proplists:get_value(KeyVal, Buffer),
   FunPoint = Point#data_point{fields = Fields#{<<"__state">> => StatePoint#data_point.fields}},
%%      flowdata:set_field(Point, <<"__state">>, StatePoint#data_point.fields),
%%   lager:notice("FunPoint is: ~p",[FunPoint]),
   case catch(faxe_lambda:execute(FunPoint, Fun)) of
      true -> replace(KeyVal, Point, State);
      _ -> {false, Buffer}
   end.



maybe_emit(true, State = #state{emit_interval = undefined}) ->
   do_emit(State);
maybe_emit(_, State = #state{}) ->
   {ok, State}.

do_emit(State = #state{buffer = Buff}) ->
   Points0 = [keep(Val, State) || {_Key, Val} <- Buff],
%%   lager:notice("emit: ~p", [[Key || {Key, _Val} <- Buff]]),
   %% sort elements by timestamp for data_batch
   Points = lists:keysort(2, Points0),
   Batch = #data_batch{points = Points},
   {emit, Batch, State}.

maybe_start_emit_timeout(#state{emit_interval = undefined}) ->
   ok;
maybe_start_emit_timeout(#state{emit_interval = Intv}) ->
   erlang:send_after(Intv, self(), emit_timeout).

keep(DataPoint, #state{keep = undefined}) ->
   DataPoint;
keep(DataPoint, #state{keep = FieldNames, keep_as = Aliases}) when is_list(FieldNames) ->
   lager:notice("keep from: ~p", [DataPoint]),
   Fields = flowdata:fields(DataPoint, FieldNames),
   lager:notice("want tp keep: ~p :: ~p as ~p",[FieldNames, Fields, Aliases]),
   Point0 = DataPoint#data_point{fields = #{}, tags = # {}},
   NewPoint0 = flowdata:set_fields(Point0, Aliases, Fields),
   NewPoint0.

add(Key, Point, #state{buffer = Buffer0, type = ?TYPE_SET}) ->
   case proplists:get_value(Key, Buffer0) of
      undefined ->
%%         lager:warning("add: ~p", [Key]),
         {true, [{Key, Point} | Buffer0]};
      _ ->
%%         lager:info("already exists: ~p", [Key]),
         {false, Buffer0}
   end;
add(Key, Point, #state{buffer = Buffer0, type = ?TYPE_LIST}) ->
%%   lager:warning("add: ~p", [Key]),
   {true, [{Key, Point} | Buffer0]}.

remove(Key, #state{buffer = Buffer0}) ->
%%   lager:warning("remove: ~p", [Key]),
   proplists:delete(Key, Buffer0).

update(Key, Point, State = #state{update = Fun, buffer = Buffer}) when is_function(Fun) ->
   case catch(faxe_lambda:execute(Point, Fun)) of
      true -> replace(Key, Point, State);
      _ -> {false, Buffer}
   end;
update(Key, Point, S = #state{update = ?UPDATE_ALWAYS}) ->
   replace(Key, Point, S).

replace(Key, Point, State = #state{buffer = Buffer}) ->
%%   lager:warning("update: ~p",[Key]),
   {true, [{Key, Point}|proplists:delete(Key, Buffer)]}.

%%%%%%%%%%%%%%%%%% TESTS %%%%%%%%%%%%%%%%%%%%%%%%%%
-ifdef(TEST).

state() ->
   RemoveFun = lambda_tests:lambda_helper("Data_mode == 0", ["data_mode"]),
   AddFun = lambda_tests:lambda_helper("Data_mode > 0", ["data_mode"]),
   #state{fields = [<<"data_code_id">>], add_function = AddFun, remove_function = RemoveFun,
      update = ?UPDATE_NEVER, buffer = []}.

point(Idx, Mode) ->
   #data_point{ts = Idx, fields = #{<<"data_code_id">> => Idx, <<"data_mode">> => Mode} }.

basic_test() ->
   State = state(),
   {_Changed1, Buffer1} = R1 = do_process(point(1,1), State),
   State1 = State#state{buffer = Buffer1},
   ?assertEqual({true, [{1, point(1, 1)}]}, R1)
   ,
   {_Changed2, Buffer2} = R2 = do_process(point(1,1), State1),
   State2 = State#state{buffer = Buffer2},
   ?assertEqual({false, [{1, point(1, 1)}]}, R2)
   ,
   {_, Buffer3} = R3 = do_process(point(2,0), State2),
   State3 = State#state{buffer = Buffer3},
   ?assertEqual({false, [{1, point(1, 1)}]}, R3)
   ,
   R4 = do_process(point(1,0), State3),
   ?assertEqual({true, []}, R4)
.



-endif.