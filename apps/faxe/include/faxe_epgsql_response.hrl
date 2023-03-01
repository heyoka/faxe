%%%-------------------------------------------------------------------
%%% @author heyoka
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 07. Nov 2021 18:55
%%%-------------------------------------------------------------------
-author("heyoka").

-record(faxe_epgsql_response, {
  time_field :: undefined | binary(),
  response_type = batch :: atom(),
  point_root_object :: undefined | binary(),
  default_timestamp :: non_neg_integer(),
  field_names_validated = false :: true|false
}).

-type faxe_epgsql_response() :: #faxe_epgsql_response{}.
-export_type([faxe_epgsql_response/0]).