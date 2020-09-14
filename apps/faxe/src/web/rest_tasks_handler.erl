%%%-------------------------------------------------------------------
%%% @author $author
%%% @copyright (C) $year, $company
%%% @doc
%%%
%%% @end
%%% Created : $fulldate
%%%-------------------------------------------------------------------
-module(rest_tasks_handler).

-include("faxe.hrl").

%%
%% Cowboy callbacks
-export([
   init/2
   , allowed_methods/2, list_json/2, content_types_provided/2, update_json/2]).

%%
%% Additional callbacks
-export([]).

-record(state, {mode}).

init(Req, [{op, Mode}]) ->
   {cowboy_rest, Req, #state{mode = Mode}}.

allowed_methods(Req, State) ->
    Value = [<<"GET">>, <<"OPTIONS">>],
    {Value, Req, State}.


content_types_provided(Req, State=#state{mode = update}) ->
   {[
      {{<<"application">>, <<"json">>, []}, update_json}
   ], Req, State};
content_types_provided(Req, State) ->
    {[
       {{<<"application">>, <<"json">>, []}, list_json}
    ], Req, State}.


list_json(Req, State=#state{mode = Mode}) ->
   #{orderby := OrderBy, dir := Direction} =
      cowboy_req:match_qs([{orderby, [], <<"changed">>}, {dir, [], <<"desc">>}], Req),
   Tasks =
      case Mode of
         list -> faxe:list_tasks();
         list_running -> faxe:list_running_tasks();
         list_by_template ->
            TId = cowboy_req:binding(template_id, Req),
            L = faxe:list_tasks_by_template(binary_to_integer(TId)),
            case L of
               {error, not_found} -> {error, template_not_found};
               _ -> L
            end;
         list_by_tags ->
            Tags = cowboy_req:binding(tags, Req),
            TagList = binary:split(Tags,[<<",">>, <<" ">>],[global, trim_all]),
            faxe:list_tasks_by_tags(TagList)
          end,
   case Tasks of
      {error, What} ->
         {jiffy:encode(#{<<"error">> => rest_helper:to_bin(What)}), Req, State};
      _ ->

         Sorted = lists:sort(order_fun(OrderBy, Direction), Tasks),
         Maps = [rest_helper:task_to_map(T) || T <- Sorted],
         {jiffy:encode(Maps), Req, State}
   end.

update_json(Req, State) ->
   Resp =
   case faxe:update_all() of
      R when is_list(R) ->
         L = integer_to_binary(length(R)),
         #{<<"success">> => true, <<"message">> => <<"updated ", L/binary, " flows">>};
      _ ->
         #{<<"success">> => false}
   end,
   {jiffy:encode(Resp), Req, State}.

order_fun(<<"id">>, Dir) ->
   fun(#task{id = AId}, #task{id = BId}) -> (AId =< BId) == order_dir(Dir) end;
order_fun(<<"name">>, Dir) ->
   fun(#task{name = AId}, #task{name = BId}) -> (AId =< BId) == order_dir(Dir) end;
order_fun(<<"last_start">>, Dir) ->
   fun(#task{last_start = AId}, #task{last_start = BId}) -> (AId =< BId) == order_dir(Dir) end;
order_fun(<<"last_stop">>, Dir) ->
   fun(#task{last_stop = AId}, #task{last_stop = BId}) -> (AId =< BId) == order_dir(Dir) end;
order_fun(_, Dir) ->
   fun(#task{date = AId}, #task{date = BId}) -> (AId =< BId) == order_dir(Dir) end.

order_dir(<<"asc">>) -> true;
order_dir(_) -> false.

