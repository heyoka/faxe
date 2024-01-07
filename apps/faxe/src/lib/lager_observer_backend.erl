-module(lager_observer_backend).

-behaviour(gen_event).

-include("faxe.hrl").
%% gen_event callbacks
-export([init/1
   , handle_call/2
   , handle_event/2
   , handle_info/2
   , terminate/2
   , code_change/3]).

-record(state, {
   level  :: {mask, integer()},
   flow_ids = []
}).

%% delay the start of our mqtt publisher
-define(START_DELAY, 200).
-define(FLOW_LIST_UPDATE_INTERVAL, 2000).

%%==============================================================================
%% gen_event callbacks
%%==============================================================================

init(Args) ->
   Level = proplists:get_value(level, Args, warning),
   {ok, #state{level = Level}}.

handle_call(get_loglevel, State = #state{level = Level}) ->
   {ok, Level, State};
handle_call({set_loglevel, Level}, State) ->
   {ok, ok, State#state{level = lager_util:config_to_mask(Level)}};
handle_call(_Request, State) ->
   {ok, ok, State}.
handle_event({log, Message}, State = #state{level = Level}) ->
   case lager_util:is_loggable(Message, Level, ?MODULE) of
      true ->
         %% we only log messages concerning dataflows
         MetaData = lager_msg:metadata(Message),
         case proplists:get_value(flow, MetaData) of
            undefined ->
               ok;
            FlowId ->
               %% get receiver pid
               Rec = faxe_flow_observer:get_observer(FlowId),
               case Rec of
                  undefined ->
                     ok;
                  _ ->
                     NodeId = proplists:get_value(comp, MetaData),
                     notify(Rec, FlowId, NodeId, Message, State)
               end
         end;
      false ->
         ok
   end,
   {ok, State};
handle_event(_Event, State) ->
   {ok, State}.

handle_info(_, State) ->
   {ok, State}.

terminate(_, #state{}) -> ok.

code_change(_, State, _) ->
   {ok, State}.

%%==============================================================================
%% Internal functions
%%==============================================================================

notify(_Receiver, _F, undefined, _, _S) ->
   ok;
notify(Receiver, _FlowId, _NodeId, Message, #state{}) ->
%%   io:format("~npublish: ~p",[{Topic, flowdata:to_json(format_data(Message))}]),
   catch Receiver ! {log, flowdata:to_json(format_data(Message))}.


%%-spec format_data(Message) -> Data when
%%   Message :: lager_msg:lager_msg(),
%%   Fields  :: [{atom(), jsx:json_term()}],
%%   Data    :: jsx:json_term().
format_data(Message) ->
   Metadata = lager_msg:metadata(Message),
   Flow = proplists:get_value(flow, Metadata, null),
   Comp = proplists:get_value(comp, Metadata, null),
   M0 = proplists:delete(flow, Metadata),
   M1 = proplists:delete(comp, M0),
%%   io:format("~nMetaData: ~p~n",[Metadata]),
   AllFieldsMeta = safe_fields(M1),
   MetaMap = maps:from_list(AllFieldsMeta),
   D = #{
%%      <<"ts">> => format_timestamp(lager_msg:timestamp(Message)),
      <<"level">> => lager_msg:severity(Message),
      <<"flow_id">> => Flow,
      <<"node_id">> => Comp,
      <<"message">> => unicode:characters_to_binary(lager_msg:message(Message)),
      <<"meta">> => MetaMap
   },
   #data_point{ts = format_timestamp(lager_msg:timestamp(Message)), fields = D}.

-spec safe_fields([{term(), term()}]) -> [{atom() | binary(), jsx:json_term()}].
safe_fields(KVPairs) ->
   lists:map(fun safe_field/1, KVPairs).

-spec safe_field({term(), term()}) -> {atom() | binary(), jsx:json_term()}.
safe_field({Key, Value}) when is_atom(Key);
   is_binary(Key)->
   {Key, safe_value(Value)};
safe_field({Key, Value}) when is_list(Key) ->
   safe_field({list_to_binary(Key), Value}).

-spec safe_value(term()) -> jsx:json_term().
safe_value(Pid) when is_pid(Pid) ->
   list_to_binary(pid_to_list(Pid));
safe_value(List) when is_list(List) ->
   case io_lib:char_list(List) of
      true ->
         list_to_binary(List);
      false ->
         lists:map(fun safe_value/1, List)
   end;
%% encode {line,char} tuple
safe_value({V1, V2} = Line) when is_integer(V1) andalso is_integer(V2) ->
   list_to_binary(lists:flatten(io_lib:format("~p",[Line])));
safe_value(Val) when is_function(Val) ->
   io:format("safe_value: value is a function!~n"),
   Val();
safe_value(Val) ->
   Val.

-spec format_timestamp(erlang:timestamp()) -> binary().
format_timestamp(Ts = {_, _, _Ms}) ->
   {_, _, Micro} = Ts,
   {Date, {Hours, Minutes, Seconds}} = calendar:now_to_universal_time(Ts),
   MsDateTime = {Date, {Hours, Minutes, Seconds, Micro div 1000 rem 1000}},
   faxe_time:to_ms(MsDateTime).

