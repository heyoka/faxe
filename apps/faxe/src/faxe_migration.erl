%%%-------------------------------------------------------------------
%%% @author heyoka
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 29. Jan 2021 11:50
%%%-------------------------------------------------------------------
-module(faxe_migration).
-author("heyoka").

-include("faxe.hrl").

%% API
-export([migrate/0]).

migrate() ->
  ok
%%  maybe_migrate_task_table()
.


maybe_migrate_task_table() ->
  NewFields = record_info(fields,  task),
  case mnesia:table_info(task, attributes) of
    NewFields -> lager:info("no table transform needed !"), ok;
    _ -> do_migrate_task_table()
  end.


do_migrate_task_table() ->
  lager:notice("transform task table"),
  mnesia:wait_for_tables(task, 5000),
  NewFields = record_info(fields,  task),
      Fun = fun( {task,
        Id,
        Name,
        Dfs,
        Definition,
        Date,
        Pid,
        Last_start,
        Last_stop,
        Permanent,
        Is_running,
        Template_vars,
        Template,
        Tags} ) ->
        {task,
          Id,
          Name,
          Dfs,
          Definition,
          Date,
          Pid,
          Last_start,
          Last_stop,
          Permanent,
          Is_running,
          Template_vars,
          Template,
          Tags,
          Name, %% group
          true %% group_leader
          }
            end,
  {atomic, ok} = mnesia:transform_table(task, Fun, NewFields),
  %% add index to group-fieldll

  mnesia:add_table_index(task, group),
  ok
.

