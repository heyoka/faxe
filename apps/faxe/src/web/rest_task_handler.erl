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
   resource_exists/2, content_types_accepted/2
   %,
   %allow_missing_post/2
]).

%%
%% Additional callbacks
-export([
   from_register_task/2, get_to_json/2
   , from_update_to_json/2, create_to_json/2]).

-include("faxe.hrl").

-record(state, {mode, task_id, task}).

init(Req, [{op, Mode}]) ->
   lager:notice("Cowboy Opts are : ~p",[Mode]),
   TId = cowboy_req:binding(task_id, Req),
   {cowboy_rest, Req, #state{mode = Mode, task_id = TId}}.

allowed_methods(Req, State) ->
    Value = [<<"GET">>, <<"OPTIONS">>, <<"PUT">>, <<"POST">>],
    {Value, Req, State}.

%%allow_missing_post(Req, State) ->
%%    Value = true,
%%    {Value, Req, State}.

%%charsets_provided(Req, State) ->
%%    % Example: 
%%    % Value = [{{ <<"text">>, <<"json">>, '*'}, from_json}],
%%    Value = skip,
%%    {Value, Req, State}.

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
content_types_provided(Req0 = #{method := _Method}, State=#state{mode = _Mode}) ->
   {[
      {{<<"application">>, <<"json">>, []}, create_to_json},
      {{<<"text">>, <<"html">>, []}, create_to_json}
   ], Req0, State}.
%%.

%%delete_completed(Req, State) ->
%%    Value = true,
%%    {Value, Req, State}.

%%delete_resource(Req, State) ->
%%    Value = false,
%%    {Value, Req, State}.

%%expires(Req, State) ->
%%    Value = undefined,
%%    {Value, Req, State}.

%%forbidden(Req, State) ->
%%    Value = false,
%%    {Value, Req, State}.

%%generate_etag(Req, State) ->
%%    Value = undefined,
%%    {Value, Req, State}.

%%is_authorized(Req, State) ->
%%    Value = true,
%%    {Value, Req, State}.

%%is_conflict(Req, State) ->
%%    Value = false,
%%    {Value, Req, State}.

%%known_methods(Req, State) ->
%%    Value = [
%%             <<"GET">>,
%%             <<"HEAD">>,
%%             <<"POST">>,
%%             <<"PUT">>,
%%             <<"PATCH">>,
%%             <<"DELETE">>,
%%             <<"OPTIONS">>
%%            ],
%%    {Value, Req, State}.

%%languages_provided(Req, State) ->
%%    Value = skip,
%%    {Value, Req, State}.

%%last_modified(Req, State) ->
%%    Value = undefined,
%%    {Value, Req, State}.

%%malformed_request(Req, State) ->
%%    Value = false,
%%    {Value, Req, State}.

%%moved_permanently(Req, State) ->
%%    Value = false,
%%    {Value, Req, State}.

%%moved_temporarily(Req, State) ->
%%    Value = false,
%%    {Value, Req, State}.

%%multiple_choices(Req, State) ->
%%    Value = false,
%%    {Value, Req, State}.

%%options(Req, State) ->
%%    Value = ok,
%%    {Value, Req, State}.

%%previously_existed(Req, State) ->
%%    Value = false,
%%    {Value, Req, State}.

%% check for existing resource only with get req
resource_exists(Req = #{method := <<"GET">>}, State=#state{mode = get, task_id = TId}) ->
   lager:warning("resource_exists? ~p",[State]),
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

%%service_available(Req, State) ->
%%    Value = true,
%%    {Value, Req, State}.

%%uri_too_long(Req, State) ->
%%    Value = false,
%%    {Value, Req, State}.

%%valid_content_headers(Req, State) ->
%%    Value = true,
%%    {Value, Req, State}.

%%valid_entity_length(Req, State) ->
%%    Value = true,
%%    {Value, Req, State}.

%%variances(Req, State) ->
%%    Value = [],
%%    {Value, Req, State}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% costum CALLBACKS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


get_to_json(Req, State=#state{task = Task}) ->
   Map = rest_helper:task_to_map(Task),
   {jsx:encode(Map), Req, State}.

from_register_task(Req, State) ->
   RespMap = #{id => 1232353, name => <<"task_name">>},
   Req2 = cowboy_req:set_resp_body(jsx:encode(RespMap), Req),
   {true, Req2, State}.
%%   {ok, Result, Req2} = cowboy_req:read_urlencoded_body(Req),
%%
%%   lager:info("~p called, ~p", [?FUNCTION_NAME, Result]),
%%   faxe:register_string_task(Result, <<"mytask1">>),
%%   {true, Req2, State}.


from_update_to_json(Req, State) ->
   RespMap = #{update => true, id => 1232353, name => <<"same_task_name">>},
   Req2 = cowboy_req:set_resp_body(jsx:encode(RespMap), Req),
   {true, Req2, State}.

create_to_json(Req, State) ->
   {stop, Req, State}.
