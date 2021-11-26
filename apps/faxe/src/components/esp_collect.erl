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
   , wants/0, emits/0, handle_info/2, check_options/0, do_process/2, shutdown/1]).

-define(UPDATE_NEVER, <<"never">>).
-define(UPDATE_ALWAYS, <<"always">>).

-define(UPDATE_MODE_REPLACE, <<"replace">>).
-define(UPDATE_MODE_MERGE, <<"merge">>).

-define(TAG_ADDED, <<"added">>).
-define(TAG_REMOVED, <<"removed">>).

-record(state, {
   node_id,
   buffer = undefined, %% holds a proplist with {key, point} -> current collection buffer

   add_function :: function(),
   remove_function :: undefined | function(),
   update_function :: undefined | function(),
   update_mode,
   emit_interval,
   fields
   ,keep
   ,keep_as,
   as,
   max_age,
   age_timer :: reference(),
   %% tagging and output
   tag_added = false :: true | false,
   include_removed = false :: true | false,
   tag_value :: term(),
   current_batch_start :: faxe_time:timestamp()
}).

options() -> [
   {key_fields, string_list},
   {add, lambda},
   {remove, lambda},
   {update, lambda, undefined},
   {update_mode, string, ?UPDATE_MODE_REPLACE}, %% 'replace', 'merge'
   {emit_every, duration, undefined},
   {tag_added, boolean, false},
   {include_removed, boolean, false},
   {keep, string_list, []}, %% a list of field path to keep for every data_point
   {keep_as, string_list, []}, %% rename the kept fields
   {as, string, <<"collected">>}, %% rename the whole field construct on output
   {max_age, duration, <<"3h">>},
   {tag_value, any, 1}
].

check_options() ->
   [
      {same_length, [keep, keep_as]},
      {func, update,
         fun(Val) ->
            is_function(Val) orelse Val == ?UPDATE_NEVER orelse Val == ?UPDATE_ALWAYS
         end,
         <<" can only be 'never', 'always' or a lambda expression">>},
      {one_of, update_mode, [?UPDATE_MODE_MERGE, ?UPDATE_MODE_REPLACE]}
   ].

wants() -> point.
emits() -> batch.

init(NodeId, _Ins, #{
   key_fields := Fields, add := AddFunc, remove := RemFunc, update := UpStateFun,
   update := UpStateFun, update_mode := UpMode,
   emit_every := EmitEvery, tag_added := TagAdd, include_removed := InclRem, tag_value := TagVal,
   keep := Keep, keep_as := KeepAs,
   as := As, max_age := MaxAge0}
) ->

   EmitInterval = case EmitEvery of undefined -> undefined; _ -> faxe_time:duration_to_ms(EmitEvery) end,
   MaxAge = case MaxAge0 of undefined -> undefined; Age -> faxe_time:duration_to_ms(Age) end,
   Aliases = case KeepAs of [] -> Keep; _ -> KeepAs end,
   {ok, all,
      #state{
         node_id = NodeId,
         fields = Fields,
         add_function = AddFunc,
         remove_function = RemFunc,
         update_function = UpStateFun,
         update_mode = UpMode,
         emit_interval = EmitInterval,
         keep = Keep,
         keep_as = Aliases,
         as = As,
         max_age = MaxAge,
         tag_added = TagAdd,
         include_removed = InclRem,
         tag_value = TagVal}}.

process(Port, Item, State = #state{buffer = undefined}) ->
   maybe_start_emit_timeout(State),
   start_age_timeout(State),
   process(Port, Item, State#state{buffer = []});
process(_Port, #data_point{} = Point, State = #state{fields = _Field}) ->
   {T, Res} = timer:tc(?MODULE, do_process, [Point, State]),
   lager:info("Took: ~p my",[T]),
   case Res of
      {ok, State} -> {ok, State};
      {Changed, NewState} ->
         maybe_emit(Changed, NewState)
   end;
process(Port, Batch = #data_batch{}, State = #state{buffer = undefined}) ->
   process(Port, Batch, State#state{buffer = []});
process(_Port, B = #data_batch{points = Points}, State) ->
   NewState0 = maybe_get_batch_start(B, State),
   ProcessFun =
   fun(Point, {Changed0, State0}) ->
      {Changed1, NewState} = do_process(Point, State0),
      ChangedNew = (Changed0 == true orelse Changed1 == true),
      {ChangedNew, NewState}
   end,
   {T, {Changed, ResState}} = timer:tc(lists, foldl, [ProcessFun, {false, NewState0}, Points]),
   lager:notice("it took ~p my to process ~p points (changed: ~p)",[T, length(Points), Changed]),
   maybe_emit(Changed, ResState).

handle_info(emit_timeout, State = #state{}) ->
   maybe_start_emit_timeout(State),
   do_emit(State);
handle_info(age_timeout, State = #state{}) ->
   start_age_timeout(State),
   {ok, age_cleanup(State)}.

shutdown(_State) ->
   ok.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
maybe_get_batch_start(#data_batch{start = undefined}, State) ->
   State;
maybe_get_batch_start(#data_batch{start = BatchStart}, State) ->
   State#state{current_batch_start = BatchStart}.

do_process(#data_point{} = Point, State = #state{buffer = _Buffer}) ->
   case is_in(Point, State) of
      {undefined, _} ->
%%         lager:notice("--- no keyval for ~p",[Point]),
         {ok, State};
      {false, KeyVal} ->
%%         lager:notice("maybe_add: ~p",[Point]),
         maybe_add(Point, KeyVal, State);
      {true, KeyVal} ->
%%         lager:notice("maybe_update: ~p",[Point]),
         {ChangedBool, NewState} = maybe_update_state(Point, KeyVal, State),
         case ChangedBool of
            true -> {true, NewState};
            false ->
%%               lager:notice("maybe_remove: ~p",[Point]),
               maybe_remove(Point, KeyVal, NewState)
         end
   end.

-spec is_in(#data_point{}, #state{}) -> {undefined|true|false, KeyVal :: term()}.
is_in(#data_point{} = Point, State = #state{buffer = Buffer, tag_value = TagVal}) ->
   KeyVal = keyval(Point, State),
   R =
      case KeyVal of
         undefined -> undefined;
         _ ->
            %% get the point
            case buffer_get(KeyVal, Buffer) of
               undefined -> false;
               P ->
                  flowdata:field(P, ?TAG_REMOVED) /= TagVal
            end
      end,
%%   lager:notice("keyval ~p", [KeyVal]),
   {R, KeyVal}.

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
         %lager:info("add: ~p ~p",[KeyVal, Point]),
         add(KeyVal, Point, State);
      _ -> {false, State}
   end.

maybe_remove(Point = #data_point{ts = _Ts}, KeyVal, State = #state{remove_function = RemFunc}) ->
   case catch(faxe_lambda:execute(Point, RemFunc)) of
      true ->
         %lager:info("remove: ~p ~p",[KeyVal, Point]),
         {true, remove1(KeyVal, State)};
      _ -> {false, State}
   end.

remove1(Key, State = #state{buffer = Buffer, include_removed = false}) ->
   State#state{buffer = buffer_delete(Key, Buffer)};
remove1(Key, State = #state{buffer = Buffer0, include_removed = true, tag_value = Tag}) ->
   RPoint = buffer_get(Key, Buffer0),
   State#state{buffer = buffer_update(Key, flowdata:set_field(RPoint, ?TAG_REMOVED, Tag), Buffer0)}.


maybe_update_state(_Point, _KeyVal, S = #state{update_function = undefined}) ->
   {false, S};
maybe_update_state(Point=#data_point{fields = Fields}, KeyVal, State = #state{update_function = Fun, buffer = Buffer}) ->
   StatePoint = buffer_get(KeyVal, Buffer),
   FunPoint = Point#data_point{fields = Fields#{<<"__state">> => StatePoint#data_point.fields}},
%%      flowdata:set_field(Point, <<"__state">>, StatePoint#data_point.fields),
%%   lager:notice("FunPoint is: ~p",[FunPoint]),
   case catch(faxe_lambda:execute(FunPoint, Fun)) of
      true ->
%%         lager:info("update: ~p ~p",[KeyVal, Point]),
         {true, do_update(KeyVal, StatePoint, Point, State)};
      _ -> {false, State}
   end.



maybe_emit(true, State = #state{emit_interval = undefined}) ->
   do_emit(State);
maybe_emit(_, State = #state{}) ->
   {ok, State}.

do_emit(State = #state{buffer = Buff, tag_value = TagVal}) ->
%%   lager:notice("buffer length before emit: ~p", [length(Buff)]),
%%   CFun =
%%   fun({_, P, _}, {Added, Removed}) ->
%%%%      lager:info("~p",[P]),
%%      NewAdded =
%%      case flowdata:field(P, ?TAG_ADDED) of
%%         TagVal -> Added+1;
%%         _ -> Added
%%      end,
%%      case flowdata:field(P, ?TAG_REMOVED) == undefined of
%%         TagVal -> {NewAdded, Removed+1};
%%         _ -> {NewAdded, Removed}
%%      end
%%   end,
%%   {A, R} = lists:foldl(CFun, {0,0}, Buff),
%%   Kept = length(Buff) - A - R,
%%   lager:notice("Added: ~p, Kept: ~p, Removed: ~p",[A, Kept, R]),

   Points0 = [
      maybe_rewrite_ts(
         keep(Val, State),
         State)
      || {_Key, Val, _} <- Buff],

   %% sort elements by timestamp for data_batch
   Points = lists:keysort(2, Points0),
   Batch = #data_batch{points = Points},
   %%
   %% now cleanup removed and added tags
   NewState = buffer_cleanup(State),
%%   lager:notice("buffer length after emit: ~p",[length(NewState#state.buffer)]),
   {emit, Batch, NewState#state{current_batch_start = undefined}}.

buffer_cleanup(State = #state{tag_added = false, include_removed = false}) ->
   State;
buffer_cleanup(State = #state{buffer = Buff, tag_value = TagVal}) ->
   CleanFun =
      fun({Key, Point, _Ts}, Acc) ->
         Point0 = flowdata:delete_field(Point, ?TAG_ADDED),
         case flowdata:field(Point0, ?TAG_REMOVED) of
            TagVal -> buffer_delete(Key, Acc);
            _ -> buffer_update(Key, Point0, Acc)
         end
      end,
   CleanedBuffer = lists:foldl(CleanFun, Buff, Buff),
   State#state{buffer = CleanedBuffer}.

age_cleanup(State = #state{buffer = Buffer, max_age = Age}) ->
   Now = faxe_time:now(),
   CleanFun =
      fun({Key, _Point, TimeAdded}, Acc) ->
         case (TimeAdded + Age) > Now of
            true -> buffer_delete(Key, Acc);
            false -> Acc
         end
      end,
   CleanedBuffer = lists:foldl(CleanFun, Buffer, Buffer),
   lager:notice("aged: ~p", [length(Buffer) - length(CleanedBuffer)]),
   State#state{buffer = CleanedBuffer}.


maybe_start_emit_timeout(#state{emit_interval = undefined}) ->
   ok;
maybe_start_emit_timeout(#state{emit_interval = Intv}) ->
   erlang:send_after(Intv, self(), emit_timeout).

start_age_timeout(#state{max_age = Age}) ->
   Interval = erlang:round(Age/2),
   lager:warning("age_interval = ~p",[Interval]),
   erlang:send_after(Interval, self(), age_timeout).

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

maybe_rewrite_ts(Point, #state{current_batch_start = undefined}) ->
   Point;
maybe_rewrite_ts(Point, #state{current_batch_start = Ts}) ->
   Point#data_point{ts = Ts}.

add(Key, Point, S = #state{buffer = Buffer0, tag_added = false}) ->
   {true, S#state{buffer = buffer_add(Key, Point, Buffer0)}};
add(Key, Point, S = #state{buffer = Buffer0, tag_added = true, tag_value = Tag}) ->
%%   {true, S#state{buffer = [{Key, flowdata:set_field(Point, ?TAG_ADDED, 1)} | Buffer0]}}.
   {true, S#state{buffer = buffer_add(Key, flowdata:set_field(Point, ?TAG_ADDED, Tag), Buffer0)}}.
%%   case buffer_get(Key, Buffer0) of
%%      undefined ->
%%%%         lager:warning("add: ~p", [Key]),
%%         case flowdata:field(Point, ?TAG_ADDED) of
%%            1 -> lager:warning("marked ADDED already when marking added: ~p",[Key]);
%%            _ -> ok
%%         end,
%%         case flowdata:field(Point, ?TAG_REMOVED) of
%%            1 -> lager:warning("marked REMOVED already when marking added: ~p",[Key]);
%%            _ -> ok
%%         end,
%%         {true, S#state{buffer = [{Key, flowdata:set_field(Point, ?TAG_ADDED, 1)} | Buffer0]}};
%%      _ ->
%%%%         lager:info("already exists: ~p", [Key]),
%%         {false, S}
%%   end;
%%add(Key, Point, S = #state{buffer = Buffer0, type = ?TYPE_LIST}) ->
%%%%   lager:warning("add: ~p", [Key]),
%%   {true, S#state{buffer = [{Key, Point} | Buffer0]}}.

do_update(Key, _OldPoint, NewPoint, State = #state{update_mode = ?UPDATE_MODE_REPLACE, buffer = Buff}) ->
   State#state{buffer = buffer_update(Key, NewPoint, Buff)};
do_update(Key, OldPoint, NewPoint, State = #state{update_mode = ?UPDATE_MODE_MERGE, buffer = Buff}) ->
   Point = flowdata:merge_points([OldPoint, NewPoint]),
%%   lager:warning("~p merged with : ~p",[OldPoint, NewPoint]),
   State#state{buffer = buffer_update(Key, Point, Buff)}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
buffer_add(Key, Point, Buffer) ->
   NewBuffer = Buffer ++ [{Key, Point, faxe_time:now()}],
   NewBuffer.

buffer_delete(Key, Buffer) ->
   NewBuffer = lists:keydelete(Key, 1, Buffer),
   NewBuffer.

buffer_update(Key, Point, Buffer) ->
   {Key, _OldP, Ts} = lists:keyfind(Key, 1, Buffer),
   NewBuffer = lists:keyreplace(Key, 1, Buffer, {Key, Point, Ts}),
   NewBuffer.

-spec buffer_get(binary(), list()) -> undefined | #data_point{}.
buffer_get(Key, Buffer) ->
   case lists:keyfind(Key, 1, Buffer) of
      false -> undefined;
      {Key, Point, _TsAdded} -> Point
   end.




%%%%%%%%%%%%%%%%%% TESTS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-ifdef(TEST).

state() ->
   RemoveFun = lambda_tests:lambda_helper("Data_mode == 0", ["data_mode"]),
   AddFun = lambda_tests:lambda_helper("Data_mode > 0", ["data_mode"]),
   #state{fields = [<<"data_code_id">>], add_function = AddFun, remove_function = RemoveFun,
      update_function = ?UPDATE_NEVER, buffer = []}.

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


tag_buffer(TagVal) ->
   F = fun(Seq) ->
      {Seq, #data_point{
         ts = Seq,
         fields =
         #{<<"name">> => iolist_to_binary([<<"point_">>, integer_to_binary(Seq)]),
            <<"data_code_id">> => Seq
         }
      } , Seq}
       end,
   [{1, P=#data_point{}, 1} | L] = lists:map(F, lists:seq(1,7)),
   [{1, flowdata:set_field(P, ?TAG_REMOVED, TagVal), 1} | L].

buffer() ->
   F = fun(Seq) ->
      {Seq, #data_point{
         ts = Seq,
         fields =
            #{<<"name">> => iolist_to_binary([<<"point_">>, integer_to_binary(Seq)]),
               <<"data_code_id">> => Seq
               }
      } , Seq}
         end,
   lists:map(F, lists:seq(1,7)).
%%   [{Key, iolist_to_binary([<<"point_">>, integer_to_binary(Key)]), Key} || Key <- lists:seq(1,7)].

buffer_add_test() ->
   Buffer0 = buffer(),
   Point = #data_point{ts = 8, fields = #{<<"name">> => <<"point_8">>, <<"data_code_id">> => 8}},
   Expected = Buffer0 ++ [{8, Point, faxe_time:now()}],
   ?assertEqual(Expected, buffer_add(8, Point, Buffer0)).

buffer_delete_test() ->
   Buffer0 = buffer(),
   [_Hd|Expected] = Buffer0,
   ?assertEqual(Expected, buffer_delete(1, Buffer0)).

buffer_update_test() ->
   Buffer = [
      {1, #data_point{ts=1, fields = #{<<"name">> => <<"eins">>}}, 1},
      {2, #data_point{ts=2, fields = #{<<"name">> => <<"zwei">>}}, 2}
   ],
   NewPoint = #data_point{ts=1, fields = #{<<"name">> => <<"one">>}},
   Expected = [
      {1, NewPoint, 1},
      {2, #data_point{ts=2, fields = #{<<"name">> => <<"zwei">>}}, 2}
   ],
   ?assertEqual(Expected, buffer_update(1, NewPoint, Buffer)).

is_in_true_test() ->
   Buffer = buffer(),
   State = state(),
   Point = #data_point{ts = 1, fields = #{<<"name">> => <<"point_1">>, <<"data_code_id">> => 1}},
   ?assertEqual({true, 1}, is_in(Point, State#state{buffer = Buffer, tag_value = 1})).

is_in_removed_test() ->

   Buffer = [
      {1, #data_point{ts=1, fields = #{<<"name">> => <<"eins">>, ?TAG_REMOVED => 1}}, 1},
      {2, #data_point{ts=2, fields = #{<<"name">> => <<"zwei">>}}, 2}
   ],
   State = state(),
   Point = #data_point{ts = 1, fields = #{<<"name">> => <<"point_12">>, <<"data_code_id">> => 1}},
   ?assertEqual({false, 1}, is_in(Point, State#state{buffer = Buffer, tag_value = 1})).

is_in_false_test() ->
   Buffer = buffer(),
   State = state(),
   Point = #data_point{ts = 12, fields = #{<<"name">> => <<"point_12">>, <<"data_code_id">> => 12}},
   ?assertEqual({false, 12}, is_in(Point, State#state{buffer = Buffer})).

is_in_undefined_test() ->
   Buffer = buffer(),
   State = state(),
   Point = #data_point{ts = 1, fields = #{<<"name">> => <<"point_1">>}},
   ?assertEqual({undefined, undefined}, is_in(Point, State#state{buffer = Buffer})).

is_in_tagvalue_test() ->
   TagValue = <<"ohyeah">>,
   Buffer = buffer(),
   io:format("Buffer:~p~n",[Buffer]),
   State = state(),
   Point = #data_point{ts = 1, fields = #{<<"name">> => <<"point_1">>, <<"data_code_id">> => 1}},
   ?assertEqual({true, 1}, is_in(Point, State#state{buffer = Buffer, tag_value = TagValue})).

is_in_tagvalue_false_test() ->
   TagValue = <<"ohyeah">>,
   Buffer = tag_buffer(TagValue),
   io:format("Buffer:~p~n",[Buffer]),
   State = state(),
   Point = #data_point{ts = 1, fields = #{<<"name">> => <<"point_1">>, <<"data_code_id">> => 1}},
   ?assertEqual({false, 1}, is_in(Point, State#state{buffer = Buffer, tag_value = TagValue})).

buffer_cleanup_added_test() ->
   Buffer = [
      {1, #data_point{ts=1, fields = #{<<"name">> => <<"eins">>}}, 1},
      {2, #data_point{ts=2, fields = #{<<"name">> => <<"zwei">>}}, 2},
      {3, #data_point{ts=3, fields = #{<<"name">> => <<"zwei">>, <<"added">> => 1}}, 3}
   ],
   Expected = [
      {1, #data_point{ts=1, fields = #{<<"name">> => <<"eins">>}}, 1},
      {2, #data_point{ts=2, fields = #{<<"name">> => <<"zwei">>}}, 2},
      {3, #data_point{ts=3, fields = #{<<"name">> => <<"zwei">>}}, 3}
   ],
   State = #state{buffer = Buffer, tag_added = true, tag_value = 1},

   ?assertEqual(State#state{buffer = Expected}, buffer_cleanup(State)).

buffer_cleanup_removed_test() ->
   Buffer = [
      {1, #data_point{ts=1, fields = #{<<"name">> => <<"eins">>}}, 1},
      {2, #data_point{ts=2, fields = #{<<"name">> => <<"zwei">>}}, 2},
      {3, #data_point{ts=3, fields = #{<<"name">> => <<"zwei">>, <<"removed">> => 1}}, 3},
      {4, #data_point{ts=4, fields = #{<<"name">> => <<"zwei">>, <<"added">> => 1}}, 4}
   ],
   Expected = [
      {1, #data_point{ts=1, fields = #{<<"name">> => <<"eins">>}}, 1},
      {2, #data_point{ts=2, fields = #{<<"name">> => <<"zwei">>}}, 2},
      {4, #data_point{ts=4, fields = #{<<"name">> => <<"zwei">>}}, 4}
   ],
   State = #state{buffer = Buffer, tag_added = true, include_removed = true, tag_value = 1},

   ?assertEqual(State#state{buffer = Expected}, buffer_cleanup(State)).

buffer_cleanup_nope_test() ->
   Buffer = [
      {1, #data_point{ts=1, fields = #{<<"name">> => <<"eins">>}}, 1},
      {2, #data_point{ts=2, fields = #{<<"name">> => <<"zwei">>}}, 2},
      {3, #data_point{ts=3, fields = #{<<"name">> => <<"zwei1">>, <<"removed">> => 1}}, 3},
      {4, #data_point{ts=4, fields = #{<<"name">> => <<"zwei2">>, <<"whatever">> => 1}}, 4}
   ],
   State = #state{buffer = Buffer, tag_added = false, include_removed = false, tag_value = 1},
   ?assertEqual(State, buffer_cleanup(State)).

-endif.