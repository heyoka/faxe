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
%%    _Dispatch = cowboy_router:compile([
%%       {'_', [
%%          {"/admin", faxe_web_handler, []},
%%          {"/empty", faxe_web_handler, []},
%%          {"/assets/[...]", cowboy_static, {priv_dir, faxe, "assets/"}}
%%       ]}
%%
%%    ]),
%%   {ok, _} = cowboy:start_http(http, 100, [{port, 8081}], [
%%      {env, [{dispatch, Dispatch}]}
%%   ]),

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
