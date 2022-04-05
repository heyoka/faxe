%% Date: 2.4.2022
%% Ⓒ 2022 heyoka
%% @doc
%%
%|jsn_select('streamId')
%.from(stream_lookup)
%.where({'key', "data.address"}, {'dataformat', dataFormat_src})
%.as('stream_id')
%%
%%
-module(esp_jsn_select).
-author("Alexander Minichmair").

-behaviour(df_component).

-include("faxe.hrl").

%% API
-export([init/3, process/3, options/0, wants/0, emits/0, shutdown/1]).

-record(state, {
   cache = undefined  :: list(),
   pre_value_cache = [] :: list(),
   fields = [] :: list(),
   wheres = [] :: list(),
   from        :: binary(),
   as          :: binary(),

   references = [] :: list()

}).

options() ->
   [
      {fields, string_list},
      {from, string},
      {where, tuple_list},
      {as, string_list}
   ].

wants() -> both.
emits() -> both.

init(_NodeId, _Inputs, #{fields := Fields, as := As, where := Wheres, from := From}=Opts) ->
   lager:notice("~p OPTS: ~p",[?MODULE, Opts]),
   %% get our structure first
   Jsn = faxe_lambda_lib:get_jsn(From),
   State = #state{fields = Fields, as = As, wheres = Wheres, from = Jsn},
   Refs = references(State),
   case Refs of
      [] ->
         Result = process_point(#data_point{},State),
         lager:notice("cache result, no reference given (~p)",[Result]),
         %% cache everything, we have no moving data
         State#state{cache = Result};
      _ ->
         lager:notice("References found: ~p",[Refs]),
         State#state{references = Refs}
   end,
   {ok, all, State}.

process(_, #data_point{} = Point, State=#state{} ) ->
   NewPoint = process_point(Point, State),
   {emit, NewPoint, State};
process(_, B = #data_batch{points = Points}, State=#state{} ) ->
   {NewPoints, NewState} = process_batch(Points, State),
   {emit, B#data_batch{points = NewPoints}, NewState}.

shutdown(#state{}) ->
   ok.

%%%===================================================================
%%% Internal functions
%%%===================================================================
process_batch(Points, State = #state{}) ->
   do_process_batch(Points, State, []).

do_process_batch([], State, Acc) ->
   {lists:reverse(Acc), State};
do_process_batch([Point|Points], State, Acc) ->
   {NewPoint, NewState} = do_process(Point, State),
   do_process_batch(Points, NewState, [NewPoint|Acc]).


process_point(Point, State = #state{cache = undefined}) ->
   do_process(Point, State);
process_point(Point, S=#state{cache = Cached, as = As}) ->
   lager:info("return cached"),
   {flowdata:set_field(Point, As, Cached), S}.

-spec do_process(#data_point{}, #state{}) -> {#data_point{}, #state{}}.
do_process(Point, State = #state{wheres = Wheres, fields = Fields}) ->
   do_process(Point, Wheres, Fields, State).

-spec do_process(#data_point{}, list(), list(), #state{}) -> {#data_point{}, #state{}}.
do_process(Point, [], [], State) ->
   {Point, State};
do_process(Point, [Where0|Wheres], [Field|Fields], State = #state{from = Jsn, as = As}) ->
   Where = replace(Point, Where0, State#state.references),
   lager:notice("do_process Wheres replaced:~p",[Where]),
   {NewVal, NewState} =
   case proplists:get_value(Where, State#state.pre_value_cache) of
      undefined ->
         NewValue = jsn:select({value, Field}, Where, Jsn),
         lager:info("got new value : ~p",[NewValue]),
         {NewValue, State#state{pre_value_cache = [{Where, NewValue}|State#state.pre_value_cache ]}};
      Val ->
         {Val, State}
   end,
   do_process(flowdata:set_field(Point, As, NewVal), Wheres, Fields, NewState).

do_select(Field, Where, Jsn) ->
   jsn:select({value, Field}, Where, Jsn).

references(_State = #state{wheres = Wheres}) ->
   F = fun
          ({_Field, {ref, Value}}, Acc) -> [Value|Acc];
          (_E, Acc) -> Acc
       end,
   lists:foldl(F, [], Wheres).

replace(Point, Where, Refs) ->
   lists:reverse(
   lists:foldl(fun({Key, Val} = W, Out) ->
      case lists:member(Val, Refs) of
         true -> [{Key, flowdata:field(Point, Val)}|Out];
         false -> [W|Out]
      end
               end,
      [],
      Where
      )).


