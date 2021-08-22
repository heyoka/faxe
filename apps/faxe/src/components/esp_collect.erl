%% Date: 08.20.21 - 08:57
%% Ⓒ 2021 heyoka
%%
%% @doc
%%
%% @end
%% @todo how to handle updates ? especially with list type, as there could be duplicate indices
%% @todo persist collection to disc and decided how to deal with a restart of the node
%% @todo update criterion ?
%% @todo implement 'as', 'max_age', 'keep_as'
%% @todo maybe make this node handle multiple collections ?
%% @todo other collection types possible, needed ?
%%
-module(esp_collect).
-author("Alexander Minichmair").


%% API
-behavior(df_component).

-include("faxe.hrl").
%% API
-export([init/3, process/3, options/0
%%   , check_options/0
   , wants/0, emits/0, handle_info/2]).

-record(state, {
   node_id,
   buffer = undefined,
   add_function,
   remove_function,
   emit_interval,
   field
   ,keep
   ,keep_as,
   type = <<"list">>,
   as,
   max_age
}).

options() -> [
   {key_field, string},
   {type, string, <<"list">>},
   {add, lambda},
   {remove, lambda},
   {emit_every, duration, undefined},
   {keep, string_list, undefined}, %% a list of field path to keep for every data_point
   {keep_as, string_list, undefined}, %% rename the kept fields
   {as, string, <<"collected">>}, %% rename the whole field construct on output
   {max_age, duration, undefined}
].

wants() -> point.
emits() -> batch.

init(NodeId, _Ins, #{key_field := Field, add := AddFunc, remove := RemFunc, type := Type,
   emit_every := EmitEvery, keep := Keep, keep_as := KeepAs, as := As, max_age := MaxAge0}) ->

   EmitInterval = case EmitEvery of undefined -> undefined; _ -> faxe_time:duration_to_ms(EmitEvery) end,
   MaxAge = case MaxAge0 of undefined -> undefined; Age -> faxe_time:duration_to_ms(Age) end,
   Aliases = case KeepAs of undefined -> Keep; _ -> KeepAs end,
   {ok, all,
      #state{
         node_id = NodeId,
         field = Field,
         type = Type,
         add_function = AddFunc,
         remove_function = RemFunc,
         emit_interval = EmitInterval,
         keep = Keep,
         keep_as = Aliases,
         as = As,
         max_age = MaxAge}}.

process(P, Point, State = #state{buffer = undefined}) ->
   maybe_start_emit_timeout(State),
   process(P, Point, State#state{buffer = []});
process(_Port, #data_point{} = Point, State = #state{}) ->
   NewBuffer0 = maybe_add(Point, State),
   NewBuffer = maybe_remove(Point, State#state{buffer = NewBuffer0}),
   maybe_emit(State#state{buffer = NewBuffer}).

handle_info(emit_timeout, State = #state{}) ->
   maybe_start_emit_timeout(State),
   do_emit(State).

maybe_add(Point = #data_point{ts = _Ts}, State = #state{add_function = AddFunc, field = Field, buffer = Buffer}) ->
   case catch(faxe_lambda:execute(Point, AddFunc)) of
      true -> add(flowdata:field(Point, Field), keep(Point, State), State);
      _ -> Buffer
   end.

maybe_remove(Point = #data_point{ts = _Ts}, State = #state{remove_function = RemFunc, field = Field, buffer = Buffer}) ->
   case catch(faxe_lambda:execute(Point, RemFunc)) of
      true -> remove(flowdata:field(Point, Field), State);
      _ -> Buffer
   end.

maybe_emit(State = #state{emit_interval = undefined}) ->
   do_emit(State);
maybe_emit(State = #state{}) ->
   {ok, State}.

do_emit(State = #state{buffer = Buff}) ->
   Points0 = [Val || {_Key, Val} <- Buff],
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
   Fields = flowdata:fields(DataPoint, FieldNames),
   Point0 = DataPoint#data_point{fields = #{}, tags = # {}},
   NewPoint0 = flowdata:set_fields(Point0, Aliases, Fields),
%%   flowdata:set_tags(NewPoint0, )
   NewPoint0.

add(Key, Point, #state{buffer = Buffer0, type = <<"set">>}) ->
   case proplists:get_value(Key, Buffer0) of
      undefined ->
         lager:warning("add: ~p", [Key]), [{Key, Point} | Buffer0];
      _ ->
         lager:warning("already exists: ~p", [Key]),
         Buffer0
   end;
add(Key, Point, #state{buffer = Buffer0, type = <<"list">>}) ->
   lager:warning("add: ~p", [Key]),
   [{Key, Point} | Buffer0].

remove(Key, #state{buffer = Buffer0}) ->
   lager:warning("remove: ~p", [Key]),
   proplists:delete(Key, Buffer0).