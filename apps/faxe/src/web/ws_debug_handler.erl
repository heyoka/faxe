%%%-------------------------------------------------------------------
%%% @author heyoka
%%% @copyright (C) 2023, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 23. May 2023 8:21 AM
%%%-------------------------------------------------------------------
-module(ws_debug_handler).
-author("heyoka").

-export([init/2, terminate/3]).
-export([websocket_init/1]).
-export([websocket_handle/2]).
-export([websocket_info/2]).

-define(SOCKET_IDLE_TIMEOUT, 180000).
-define(MAX_FRAME_SIZE, 5000000).

-define(WS_OPTS, #{
  idle_timeout => ?SOCKET_IDLE_TIMEOUT,
  max_frame_size => ?MAX_FRAME_SIZE}).

-record(state, {
  opts,
  username,
  authorized = false,
  conn_status_subs = [],
  debug_subs = []
}).

init(Req = #{}, Opts) ->
  State = #state{opts = Opts},
  {cowboy_websocket, Req, State, ?WS_OPTS}.

websocket_init(State = #state{}) ->
  {[], State}.



%%websocket_handle({text, W}, State) ->
%%  lager:notice("websocket_handle : ~p",[W]),
%%  {[], State};

websocket_handle({text, AuthMsg}, State = #state{authorized = false}) ->
  case catch jiffy:decode(AuthMsg, [return_maps]) of
    #{<<"user">> := User, <<"pass">> := Pass} ->
      case faxe_db:has_user_with_pw(User, Pass) of
        true ->
          lager:info("User ~p logged in",[User]),
          {[], State#state{username = User, authorized = true}};
        false ->
          case faxe_config:get(allow_anonymous, false) of
            true ->
              lager:info("User ~p logged in",[<<"anon">>]),
              {[], State#state{username = <<"anon">>, authorized = true}};
            false -> {[{close, 1000, <<"unauthorized">>}], State}
          end
      end;
    _ ->
      {[{text, jiffy:encode(rest_helper:msg_error(<<"user-pass auth frame expected">>))}], State}
  end;
websocket_handle({text, Msg}, State = #state{}) ->
  {ReturnList, NewState} =
  case catch jiffy:decode(Msg, [return_maps]) of
    DecodedMsg when is_map(DecodedMsg) -> handle_msg(DecodedMsg, State);
    _ -> {[rest_helper:msg_error(<<"json_invalid">>)], State}
  end,
  lager:info("got message: ~p",[Msg]),
  {[{text, jiffy:encode(Return)} || Return <- ReturnList], NewState};
websocket_handle(_Data, State) ->
  {[], State}.


websocket_info({conn_status, Item}, State) ->
%%  lager:notice("conn_status ~p",[Item]),
  Out = #{<<"type">> => <<"conn_status">>, <<"data">> => flowdata:to_mapstruct(Item)},
  {[{text, jiffy:encode(Out)}], State};
websocket_info({debug, Item}, State) ->
%%  lager:notice("debug message for ~p",[Item]),
  Out = #{<<"type">> => <<"debug">>, <<"data">> => flowdata:to_mapstruct(Item)},
  {[{text, jiffy:encode(Out)}], State};
websocket_info(What, State) ->
  lager:notice("ws_handler ~p got info ~p" ,[self(), What]),
  {[], State}.


terminate(Reason, _PartialReq, _State = #state{conn_status_subs = CSubs}) ->
  lager:notice("delete conn_status event_handlers: ~p due to reason ~p", [CSubs, Reason]),
  Res = [remove_conn_status_handler(FlowId) || FlowId <- CSubs],
  lager:warning("Res from deleting event_handlers ~p",[Res]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% internal
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

handle_msg(#{<<"type">> := <<"subscribe">>, <<"data">> := SubscriptionData}, State) ->
  #{<<"topics">> := Topics, <<"flow_id">> := FlowId} = SubscriptionData,
  {NewState, Messages} = handle_subscribe(Topics, FlowId, State, []),
  {Messages, NewState};
handle_msg(#{<<"type">> := <<"unsubscribe">>, <<"data">> := SubscriptionData}, State) ->
  #{<<"topics">> := Topics, <<"flow_id">> := FlowId} = SubscriptionData,
  NewState = handle_unsubscribe(Topics, FlowId, State),
  {[rest_helper:msg_success(<<"sucessfully unsubscribed">>)], NewState};
handle_msg(_Invalid, State) ->
  {[rest_helper:msg_error(<<"message invalid">>)], State}.

handle_subscribe([], _, State, Messages) ->
  {State, Messages};
handle_subscribe([<<"debug">>|SList], FlowId, State, Messages) ->
  gen_event:add_sup_handler(faxe_debug, {debug_handler, {FlowId, self()}},
    #{parent => self(), flow_id => FlowId, node_id => undefined}),
  faxe:start_trace(FlowId, undefined),
  handle_subscribe(SList, FlowId, State, Messages);
handle_subscribe([<<"conn_status">>|SList], FlowId, State = #state{conn_status_subs = CSubs}, Messages) ->
%%  lager:info("CONN STATUS for ~p: ~p",[FlowId, faxe:list_connection_status(FlowId)]),
  gen_event:add_sup_handler(conn_status, {conn_status_handler_dataflow, {FlowId, self()}},
    #{parent => self(), flow_id => FlowId, node_id => undefined}),
  InitialMessages0 = [flowdata:to_mapstruct(Item) || Item <- faxe:list_connection_status(FlowId)],
  InitialMessages = [#{<<"type">> => <<"conn_status">>, <<"data">> => Item} || Item <- InitialMessages0],
  handle_subscribe(SList, FlowId, State#state{conn_status_subs = [FlowId|CSubs]},
    InitialMessages++Messages);
handle_subscribe([_|SList], FlowId, State, Msgs) ->
  handle_subscribe(SList, FlowId, State, Msgs).

handle_unsubscribe([], _, State) ->
  State;
handle_unsubscribe([<<"debug">>|SList], FlowId, State) ->
  remove_debug_handler(FlowId),
  handle_unsubscribe(SList, FlowId, State);
handle_unsubscribe([<<"conn_status">>|SList], FlowId, State = #state{conn_status_subs = CSubs}) ->
  remove_conn_status_handler(FlowId),
  handle_unsubscribe(SList, FlowId, State#state{conn_status_subs = [FlowId|CSubs]});
handle_unsubscribe([_|SList], FlowId, State) ->
  handle_unsubscribe(SList, FlowId, State).


remove_conn_status_handler(FlowId) ->
  gen_event:delete_handler(conn_status, {conn_status_handler_dataflow, {FlowId, self()}}, stopped).

remove_debug_handler(FlowId) ->
  gen_event:delete_handler(faxe_debug, {debug_handler, {FlowId, self()}}, stopped).
