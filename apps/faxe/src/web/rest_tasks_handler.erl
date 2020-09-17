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
   , allowed_methods/2,
   list_json/2,
   content_types_provided/2,
   update_json/2,
   start_permanent_json/2,
   stop_all_json/2,
   start_json/2,
   stop_json/2, start_tags_json/2, stop_tags_json/2]).

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
content_types_provided(Req, State=#state{mode = start_permanent}) ->
   {[
      {{<<"application">>, <<"json">>, []}, start_permanent_json}
   ], Req, State};
content_types_provided(Req, State=#state{mode = start}) ->
   {[
      {{<<"application">>, <<"json">>, []}, start_json}
   ], Req, State};
content_types_provided(Req, State=#state{mode = start_by_ids}) ->
   {[
      {{<<"application">>, <<"json">>, []}, start_json}
   ], Req, State};
content_types_provided(Req, State=#state{mode = start_by_tags}) ->
   {[
      {{<<"application">>, <<"json">>, []}, start_tags_json}
   ], Req, State};
content_types_provided(Req, State=#state{mode = stop}) ->
   {[
      {{<<"application">>, <<"json">>, []}, stop_json}
   ], Req, State};
content_types_provided(Req, State=#state{mode = stop_by_ids}) ->
   {[
      {{<<"application">>, <<"json">>, []}, stop_json}
   ], Req, State};
content_types_provided(Req, State=#state{mode = stop_by_tags}) ->
   {[
      {{<<"application">>, <<"json">>, []}, stop_tags_json}
   ], Req, State};
content_types_provided(Req, State=#state{mode = stop_all}) ->
   {[
      {{<<"application">>, <<"json">>, []}, stop_all_json}
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
            tasks_by_tags(Req)
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
         Add = maybe_plural(length(R), <<"flow">>),
         #{<<"success">> => true, <<"message">> => <<"updated ", Add/binary>>};
      _ ->
         #{<<"success">> => false}
   end,
   {jiffy:encode(Resp), Req, State}.

start_permanent_json(Req, State) ->
   Resp =
      case faxe:start_permanent_tasks() of
         R when is_list(R) ->
            Add = maybe_plural(length(R), <<"permanent flow">>),
            #{<<"success">> => true, <<"message">> => <<"started ", Add/binary>>};
         _ ->
            #{<<"success">> => false}
      end,
   {jiffy:encode(Resp), Req, State}.


stop_all_json(Req, State) ->
   Running = faxe:list_running_tasks(),
   [faxe:stop_task(T) || T <- Running],
   Add = maybe_plural(length(Running), <<"flow">>),
   M = #{<<"success">> => true, <<"message">> => <<"stopped ", Add/binary>>},
   {jiffy:encode(M), Req, State}.

%% start a list of tasks (by string id)
start_json(Req, State) ->
   IdList = id_list(Req),
   StartResult = [faxe:start_task(Id) || Id <- IdList],
   Add = maybe_plural(length(StartResult), <<"flow">>),
   M = #{<<"success">> => true, <<"message">> => <<"started ", Add/binary>>},
   {jiffy:encode(M), Req, State}.

start_tags_json(Req, State) ->
   Tasks = tasks_by_tags(Req),
   StartResult = [faxe:start_task(T#task.id) || T <- Tasks],
   Add = maybe_plural(length(StartResult), <<"flow">>),
   M = #{<<"success">> => true, <<"message">> => <<"started ", Add/binary>>},
   {jiffy:encode(M), Req, State}.

%% stop a list of tasks (by string id)
stop_json(Req, State) ->
   IdList = id_list(Req),
   StopRes = [faxe:stop_task(Id) || Id <- IdList],
   Add = maybe_plural(length(StopRes), <<"flow">>),
   M = #{<<"success">> => true, <<"message">> => <<"stopped ", Add/binary>>},
   {jiffy:encode(M), Req, State}.

stop_tags_json(Req, State) ->
   Tasks = tasks_by_tags(Req),
   StartResult = [faxe:stop_task(T) || T <- Tasks],
   Add = maybe_plural(length(StartResult), <<"flow">>),
   M = #{<<"success">> => true, <<"message">> => <<"stopped ", Add/binary>>},
   {jiffy:encode(M), Req, State}.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
id_list(Req) ->
   Ids = cowboy_req:binding(ids, Req),
   binary:split(Ids,[<<",">>, <<" ">>],[global, trim_all]).

tasks_by_tags(Req) ->
   Tags = cowboy_req:binding(tags, Req),
   TagList = binary:split(Tags,[<<",">>, <<" ">>],[global, trim_all]),
   faxe:list_tasks_by_tags(TagList).

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


maybe_plural(Num, Word) when is_integer(Num), is_binary(Word) ->
   NumBin = integer_to_binary(Num),
   NewWord =
   case Num of
      1 -> Word;
      _ -> <<Word/binary, "s">>
   end,
   <<NumBin/binary, " ", NewWord/binary>>.
