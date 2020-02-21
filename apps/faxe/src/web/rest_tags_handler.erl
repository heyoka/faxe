%%%-------------------------------------------------------------------
%%% @author $author
%%% @copyright (C) $year, $company
%%% @doc
%%%
%%% @end
%%% Created : $fulldate
%%%-------------------------------------------------------------------
-module(rest_tags_handler).

-include("faxe.hrl").

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
%%   lager:notice("Cowboy Opts are : ~p",[Mode]),
   {cowboy_rest, Req, #state{mode = Mode}}.

allowed_methods(Req, State) ->
    Value = [<<"GET">>, <<"OPTIONS">>],
    {Value, Req, State}.

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
            #{tag := Tags} = cowboy_req:match_qs([{tag, [], []}])
          end,
   case Tasks of
      {error, What} ->
         {jiffy:encode(#{<<"error">> => rest_helper:to_bin(What)}), Req, State};
      _ ->
         Sorted = lists:sort(order_fun(OrderBy, Direction), Tasks),
         Maps = [rest_helper:task_to_map(T) || T <- Sorted],
         {jiffy:encode(Maps), Req, State}
   end.



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

