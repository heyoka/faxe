%%%-------------------------------------------------------------------
%%% @author $author
%%% @copyright (C) $year, $company
%%% @doc
%%%
%%% @end
%%% Created : $fulldate
%%%-------------------------------------------------------------------
-module(rest_template_handler).

%%
%% Cowboy callbacks
-export([
   init/2
   , allowed_methods/2, content_types_provided/2,
   resource_exists/2, content_types_accepted/2
   %,
   %allow_missing_post/2
   , delete_resource/2]).

%%
%% Additional callbacks
-export([
   from_register_template/2, get_to_json/2
   , create_to_json/2, from_totask/2]).

-include("faxe.hrl").

-record(state, {mode, template_id, template}).

init(Req, [{op, Mode}]) ->
   TId = cowboy_req:binding(template_id, Req),
   {cowboy_rest, Req, #state{mode = Mode, template_id = TId}}.

allowed_methods(Req, State=#state{mode = get}) ->
   {[<<"GET">>, <<"OPTIONS">>], Req, State};
allowed_methods(Req, State=#state{mode = register}) ->
   {[<<"POST">>], Req, State};
allowed_methods(Req, State=#state{mode = totask}) ->
   {[<<"POST">>], Req, State};
allowed_methods(Req, State=#state{mode = delete}) ->
   {[<<"DELETE">>], Req, State}.

%%allowed_methods(Req, State) ->
%%    Value = [<<"GET">>, <<"OPTIONS">>, <<"PUT">>, <<"POST">>, <<"DELETE">>],
%%    {Value, Req, State}.

content_types_accepted(Req = #{method := <<"POST">>}, State = #state{mode = register}) ->
    Value = [{{ <<"application">>, <<"x-www-form-urlencoded">>, []}, from_register_template}],
    {Value, Req, State};
content_types_accepted(Req = #{method := <<"POST">>} , State = #state{mode = totask}) ->
   Value = [{{ <<"application">>, <<"x-www-form-urlencoded">>, []}, from_totask}],
   {Value, Req, State}.

content_types_provided(Req, State=#state{mode = get}) ->
    {[
       {{<<"application">>, <<"json">>, []}, get_to_json},
       {{<<"text">>, <<"html">>, []}, get_to_json}
    ], Req, State};
content_types_provided(Req0 = #{method := _Method}, State=#state{mode = _Mode}) ->
   {[
      {{<<"application">>, <<"json">>, []}, create_to_json},
      {{<<"text">>, <<"html">>, []}, create_to_json}
   ], Req0, State}.
%%.


%% check for existing resource only with get req
resource_exists(Req = #{method := <<"GET">>}, State=#state{mode = get, template_id = TId}) ->
   {Value, NewState} =
    case TId of
       undefined -> {true, State};
       Id -> case faxe_db:get_template(binary_to_integer(Id)) of
                {error, not_found} -> {false, State};
                Task=#template{} -> {true, State#state{template = Task, template_id = Task#template.id}}
             end
    end,
    {Value, Req, NewState};
resource_exists(Req, State) ->
   {true, Req, State}.


delete_resource(Req, State=#state{template_id = TaskId}) ->
   lager:notice("DELETE"),
   case faxe:delete_template(binary_to_integer(TaskId)) of
      ok ->
         RespMap = #{success => true, message =>
         iolist_to_binary([<<"Template ">>, TaskId, <<" successfully deleted.">>])},
         Req2 = cowboy_req:set_resp_body(jsx:encode(RespMap), Req),
         {true, Req2, State};
      {error, Error} ->
         lager:info("Error occured when deleting template: ~p",[Error]),
         Req3 = cowboy_req:set_resp_body(jsx:encode(#{success => false, error => rest_helper:to_bin(Error)}), Req),
         {false, Req3, State}
   end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% costum CALLBACKS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


get_to_json(Req, State=#state{template = Task}) ->
   Map = rest_helper:template_to_map(Task),
   {jsx:encode(Map), Req, State}.

from_register_template(Req, State) ->
   rest_helper:do_register(Req, State, template).

from_totask(Req, State=#state{template_id = TId}) ->
   TaskName = cowboy_req:binding(task_name, Req),
   {ok, Result, Req2} = cowboy_req:read_urlencoded_body(Req),
   Json = proplists:get_value(<<"vars">>, Result, <<"[]">>),
   Vars = jsx:decode(Json),
   lager:notice("Body: ~p, Vars: ~p",[Result, Vars]),
   case faxe:task_from_template(binary_to_integer(TId), TaskName, Vars) of
      ok ->
         Req3 = cowboy_req:set_resp_body(jsx:encode(#{success => true, name => TaskName}), Req2),
         {true, Req3, State};
      {error, Error} ->
         lager:info("Error occured when generating flow from template: ~p",[Error]),
         Req3 = cowboy_req:set_resp_body(
            jsx:encode(#{success => false, error => rest_helper:to_bin(Error)}), Req),
         {false, Req3, State}
   end.

create_to_json(Req, State) ->
   {stop, Req, State}.
