%%%-------------------------------------------------------------------
%%% @author $author
%%% @copyright (C) $year, $company
%%% @doc
%%%
%%% @end
%%% Created : $fulldate
%%%-------------------------------------------------------------------
-module(rest_tasks_handler).

%%
%% Cowboy callbacks
-export([
   init/2
   , allowed_methods/2, list_json/2, content_types_provided/2]).

%%
%% Additional callbacks
-export([
]).

-record(state, {mode}).

init(Req, [{op, Mode}]) ->
   lager:notice("Cowboy Opts are : ~p",[Mode]),
   {cowboy_rest, Req, #state{mode = Mode}}.

allowed_methods(Req, State) ->
    Value = [<<"GET">>, <<"OPTIONS">>],
    {Value, Req, State}.

%%allow_missing_post(Req, State) ->
%%    Value = true,
%%    {Value, Req, State}.

%%charsets_provided(Req, State) ->
%%    % Example: 
%%    % Value = [{{ <<"text">>, <<"json">>, '*'}, from_json}],
%%    Value = skip,
%%    {Value, Req, State}.

%%content_types_accepted(Req, State) ->
%%    Value = [{{ <<"application">>, <<"x-www-form-urlencoded">>, '*'}, }],
%%    Value = none,
%%    {Value, Req, State}.

content_types_provided(Req, State) ->
%%    Value = [{{ <<"text">>, <<"html">>, '*'}, to_html}],
    {[
       {{<<"application">>, <<"json">>, []}, list_json},
       {{<<"text">>, <<"html">>, []}, list_json}
    ], Req, State}.

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

%%resource_exists(Req, State) ->
%%    Value = true,
%%    {Value, Req, State}.

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

list_json(Req, State=#state{mode = Mode}) ->
   Maps = case Mode of
             list -> L = lists:flatten(faxe:list_tasks()), [rest_helper:task_to_json(T) || T <- L];
             list_running -> L = lists:flatten(faxe:list_templates()),
                [rest_helper:template_to_json(T) || T <- L]
          end,
   {jsx:encode(Maps), Req, State}.
