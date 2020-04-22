%%%-------------------------------------------------------------------
%%% @author heyoka
%%% @copyright (C) 2020, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 10. Mär 2020 21:16
%%%-------------------------------------------------------------------
-module(faxe_config).
-author("heyoka").

%% API
-export([get/1, q_file/1]).

get(Key) ->
   application:get_env(faxe, Key, undefined).

%% @doc get the base dir for esq q-files
q_file({GraphId, NodeId}) ->
   EsqBaseDir = faxe_config:get(esq_base_dir),
   binary_to_list(<<EsqBaseDir/binary, GraphId/binary, "/", NodeId/binary>>).

