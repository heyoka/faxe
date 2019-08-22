%%%-------------------------------------------------------------------
%%% @author $author
%%% @copyright (C) $year, $company
%%% @doc
%%%
%%% @end
%%% Created : $fulldate
%%%-------------------------------------------------------------------
-module(rest_task_handler).

%%
%% Cowboy callbacks
-export([
   init/2
   , allowed_methods/2, content_types_provided/2,
   resource_exists/2, content_types_accepted/2, delete_resource/2
]).

%%
%% Additional callbacks
-export([
   from_register_task/2, get_to_json/2
   , from_update_to_json/2, create_to_json/2, start_to_json/2, stop_to_json/2]).

-include("faxe.hrl").

-record(state, {mode, task_id, task}).

init(Req, [{op, Mode}]) ->
   TId = cowboy_req:binding(task_id, Req),
   {cowboy_rest, Req, #state{mode = Mode, task_id = TId}}.

allowed_methods(Req, State=#state{mode = get}) ->
   {[<<"GET">>, <<"OPTIONS">>], Req, State};
allowed_methods(Req, State=#state{mode = register}) ->
   {[<<"POST">>], Req, State};
allowed_methods(Req, State=#state{mode = update}) ->
   {[<<"POST">>], Req, State};
allowed_methods(Req, State=#state{mode = start}) ->
   {[<<"GET">>, <<"OPTIONS">>], Req, State};
allowed_methods(Req, State=#state{mode = stop}) ->
   {[<<"GET">>, <<"OPTIONS">>], Req, State};
allowed_methods(Req, State=#state{mode = delete}) ->
   {[<<"DELETE">>], Req, State};
allowed_methods(Req, State=#state{}) ->
   {[], Req, State}.

content_types_accepted(Req = #{method := <<"POST">>}, State = #state{mode = register}) ->
    Value = [{{ <<"application">>, <<"x-www-form-urlencoded">>, []}, from_register_task}],
    {Value, Req, State};
content_types_accepted(Req = #{method := <<"POST">>} , State = #state{mode = update}) ->
   Value = [{{ <<"application">>, <<"x-www-form-urlencoded">>, []}, from_update_to_json}],
   {Value, Req, State}.


content_types_provided(Req, State=#state{mode = get}) ->
    {[
       {{<<"application">>, <<"json">>, []}, get_to_json},
       {{<<"text">>, <<"html">>, []}, get_to_json}
    ], Req, State};
content_types_provided(Req, State=#state{mode = start}) ->
   {[
      {{<<"application">>, <<"json">>, []}, start_to_json},
      {{<<"text">>, <<"html">>, []}, start_to_json}
   ], Req, State};
content_types_provided(Req, State=#state{mode = stop}) ->
   {[
      {{<<"application">>, <<"json">>, []}, stop_to_json},
      {{<<"text">>, <<"html">>, []}, stop_to_json}
   ], Req, State};
content_types_provided(Req0 = #{method := _Method}, State=#state{mode = _Mode}) ->
   {[
      {{<<"application">>, <<"json">>, []}, create_to_json},
      {{<<"text">>, <<"html">>, []}, create_to_json}
   ], Req0, State}.
%%.


%% check for existing resource only with get req
resource_exists(Req = #{method := <<"GET">>}, State=#state{mode = Mode, task_id = TId})
      when Mode == get orelse Mode == start orelse Mode == stop ->
   {Value, NewState} =
    case TId of
       undefined -> {true, State};
       Id -> case faxe_db:get_task(binary_to_integer(Id)) of
                {error, not_found} -> {false, State};
                Task=#task{} -> {true, State#state{task = Task, task_id = Task#task.id}}
             end
    end,
    {Value, Req, NewState};
resource_exists(Req, State) ->
   {true, Req, State}.

delete_resource(Req, State=#state{task_id = TaskId}) ->
   case faxe:delete_task(binary_to_integer(TaskId)) of
      ok ->
         RespMap = #{success => true, message =>
            iolist_to_binary([<<"Task ">>, TaskId, <<" successfully deleted.">>])},
         Req2 = cowboy_req:set_resp_body(jsx:encode(RespMap), Req),
         {true, Req2, State};
      {error, Error} ->
         lager:info("Error occured when deleting flow: ~p",[Error]),
         Req3 = cowboy_req:set_resp_body(jsx:encode(#{success => false, error => rest_helper:to_bin(Error)}), Req),
         {false, Req3, State}
   end.

%%malformed_request(Req, State=state#{mode = register}) ->
%%    Value = false,
%%    {Value, Req, State}.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% costum CALLBACKS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


get_to_json(Req, State=#state{task = Task}) ->
   Map = rest_helper:task_to_map(Task),
   {jsx:encode(Map), Req, State}.

from_register_task(Req, State) ->
   rest_helper:do_register(Req, State, task).

from_update_to_json(Req, State) ->
   RespMap = #{update => true, id => 1232353, name => <<"same_task_name">>},
   Req2 = cowboy_req:set_resp_body(jsx:encode(RespMap), Req),
   {true, Req2, State}.

start_to_json(Req, State = #state{task_id = Id}) ->
   case faxe:start_task(Id, is_permanent(Req)) of
      {ok, _Graph} ->
         {jsx:encode(#{<<"ok">> => <<"started">>}), Req, State};
      {error, Error} ->
         {jsx:encode(#{<<"error">> => rest_helper:to_bin(Error)}), Req, State}
   end.

stop_to_json(Req, State = #state{task_id = Id}) ->
   case faxe:stop_task(Id, is_permanent(Req)) of
      ok ->
         {jsx:encode(#{<<"ok">> => <<"stopped">>}), Req, State};
      {error, Error} ->
         {jsx:encode(#{<<"error">> => rest_helper:to_bin(Error)}), Req, State}
   end.

create_to_json(Req, State) ->
   {stop, Req, State}.

is_permanent(Req) ->
   Permanent = cowboy_req:binding(permanent, Req, <<"false">>),
   Permanent == <<"true">>.
