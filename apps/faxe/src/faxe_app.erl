%%%-------------------------------------------------------------------
%% @doc faxe public API
%% @end
%%%-------------------------------------------------------------------

-module(faxe_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

-define(PRIV_DIR, code:priv_dir(faxe)).
-define(COMPILE_OPTS, [{out_dir, ?PRIV_DIR ++ "/templates/"}]).

%%====================================================================
%% API
%%====================================================================

start(_StartType, _StartArgs) ->
  faxe_db:create(),
   %% COWBOY
    Dispatch = cowboy_router:compile([
       {'_', [
          {"/admin", faxe_web_handler, []},
          {"/empty", faxe_web_handler, []},
          {"/assets/[...]", cowboy_static, {priv_dir, faxe, "assets/"}},
          %% REST

          {"/v1/tasks", rest_tasks_handler, [{op, list}]},
          {"/v1/tasks/running", rest_tasks_handler, [{op, list_running}]},

          {"/v1/task/stats/:task_id", rest_task_handler, [{op, stats}]},
          {"/v1/task/register", rest_task_handler, [{op, register}]},
          {"/v1/task/:task_id", rest_task_handler, [{op, get}]},
          {"/v1/task/update/:task_id", rest_task_handler, [{op, update}]},
          {"/v1/task/delete/:task_id", rest_task_handler, [{op, delete}]},
          {"/v1/task/start/:task_id/[:permanent]", rest_task_handler, [{op, start}]},
          {"/v1/task/stop/:task_id/[:permanent]", rest_task_handler, [{op, stop}]},

          {"/v1/templates", rest_templates_handler, [{op, list}]},

          {"/v1/template/register", rest_template_handler, [{op, register}]},
          {"/v1/template/:template_id", rest_template_handler, [{op, get}]},
          {"/v1/template/delete/:template_id", rest_template_handler, [{op, delete}]},

          {"/v1/task/from_template/:template_id/:task_name", rest_template_handler, [{op, totask}]}

       ]}

    ]),
   HttpPort = application:get_env(faxe, http_api_port, 8081),
   {ok, _} = cowboy:start_clear(http_rest, [{port, HttpPort}],
      #{env =>  #{dispatch => Dispatch}}
   ),

%%   {ok, Templates} = application:get_env(faxe, erlydtl_templates),
%%   lager:notice("templates: ~p" ,[[[?PRIV_DIR ++ File, Mod, ?COMPILE_OPTS] || {File, Mod} <- Templates]]),
%%   [{ok, _} = erlydtl:compile_file(?PRIV_DIR ++ File, Mod, ?COMPILE_OPTS) || {File, Mod} <- Templates],
  %% ranch
   faxe_sup:start_link().

%%--------------------------------------------------------------------
stop(_State) ->
   ok.

%%====================================================================
%% Internal functions
%%====================================================================
