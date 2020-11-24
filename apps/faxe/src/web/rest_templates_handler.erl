%%%-------------------------------------------------------------------
%%% @author $author
%%% @copyright (C) $year, $company
%%% @doc
%%%
%%% @end
%%% Created : $fulldate
%%%-------------------------------------------------------------------
-module(rest_templates_handler).

-include("faxe.hrl").
%%
%% Cowboy callbacks
-export([
  init/2
  , allowed_methods/2,
  list_json/2,
  content_types_provided/2,
  allow_missing_post/2,
  content_types_accepted/2,
  from_import/2,
  is_authorized/2]).


%%
%% Additional callbacks
-export([
]).

-record(state, {mode}).

init(Req, [{op, Mode}]) ->
   {cowboy_rest, Req, #state{mode = Mode}}.

is_authorized(Req, State) ->
  rest_helper:is_authorized(Req, State).

allowed_methods(Req, State=#state{mode = import}) ->
  {[<<"POST">>], Req, State};
allowed_methods(Req, State) ->
    Value = [<<"GET">>, <<"OPTIONS">>],
    {Value, Req, State}.

allow_missing_post(Req, State = #state{mode = import}) ->
  {false, Req, State}.

content_types_accepted(Req = #{method := <<"POST">>}, State = #state{mode = import}) ->
  Value = [{{ <<"application">>, <<"x-www-form-urlencoded">>, []}, from_import}],
  {Value, Req, State}.

content_types_provided(Req, State) ->
    {[
       {{<<"application">>, <<"json">>, []}, list_json},
       {{<<"text">>, <<"html">>, []}, list_json}
    ], Req, State}.


list_json(Req, State=#state{mode = _Mode}) ->
   #{orderby := OrderBy, dir := Direction} =
      cowboy_req:match_qs([{orderby, [], <<"changed">>}, {dir, [], <<"desc">>}], Req),
   L = lists:flatten(faxe:list_templates()),
   Sorted = lists:sort(order_fun(OrderBy, Direction), L),
   Maps = [rest_helper:template_to_map(T) || T <- Sorted],
   {jiffy:encode(Maps), Req, State}.

from_import(Req, State=#state{}) ->
  {ok, Body, Req1} = cowboy_req:read_urlencoded_body(Req),
  TemplatesJson = proplists:get_value(<<"templates">>, Body),
  case (catch jiffy:decode(TemplatesJson, [return_maps])) of
    TemplatesList when is_list(TemplatesList) -> do_import(TemplatesList, Req1, State);
    _ -> Req2 = cowboy_req:set_resp_body(
      jiffy:encode(
        #{<<"success">> => false, <<"message">> => <<"Error decoding json, invalid.">>}),
      Req),
      {false, Req2, State}
  end.


do_import(TemplateList, Req, State) ->
  {Ok, Err} =
    lists:foldl(
      fun(TemplateMap = #{<<"name">> := TName}, {OkList, ErrList}) ->
        case import_template(TemplateMap) of
          {ok, _Id} -> {[TName|OkList], ErrList};
          {error, What} -> {OkList, [#{TName => faxe_util:to_bin(What)}|ErrList]}
        end
      end,
      {[],[]},
      TemplateList
    ),
  Req2 = cowboy_req:set_resp_body(
    jiffy:encode(
      #{total => length(TemplateList), successful => length(Ok),
        errors => length(Err), messages => Err}),
    Req),
  {true, Req2, State}.


import_template(#{<<"dfs">> := Dfs, <<"name">> := Name}) ->
  case faxe:register_template_string(Dfs, Name) of
    ok ->
      Id = rest_helper:get_task_or_template_id(Name, task),
      {ok, Id};
    Err -> Err
  end.



order_fun(<<"id">>, Dir) ->
   fun(#template{id = AId}, #template{id = BId}) -> (AId =< BId) == order_dir(Dir) end;
order_fun(<<"name">>, Dir) ->
   fun(#template{name = AId}, #template{name = BId}) -> (AId =< BId) == order_dir(Dir) end;
order_fun(_, Dir) ->
   fun(#template{date = AId}, #template{date = BId}) -> (AId =< BId) == order_dir(Dir) end.

order_dir(<<"asc">>) -> true;
order_dir(_) -> false.
