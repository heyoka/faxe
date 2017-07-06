%%%-------------------------------------------------------------------
%% @doc faxe public API
%% @end
%%%-------------------------------------------------------------------

-module(faxe_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

%%====================================================================
%% API
%%====================================================================

start(_StartType, _StartArgs) ->
    faxe_db:create(),
    %% COWBOY
    Dispatch = cowboy_router:compile([
        {'_', [
            {"/admin", faxe_web_handler, []},
            {"/static/[...]", cowboy_static, {priv_dir, faxe, "static/"}}
        ]}

    ]),
    {ok, _} = cowboy:start_http(http, 100, [{port, 8080}], [
        {env, [{dispatch, Dispatch}]}
    ]),


    faxe_sup:start_link().

%%--------------------------------------------------------------------
stop(_State) ->
    ok.

%%====================================================================
%% Internal functions
%%====================================================================
