%% Date: 12.04.17 - 20:23
%% Ⓒ 2017 heyoka
%%
%% @doc
%% lambda function standard library
%%
-module(faxe_lambda_lib).
-author("Alexander Minichmair").

%% API
-compile(export_all).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Time related functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

to_date(Ts) -> faxe_time:to_date(Ts).

to_date_string(Ts) ->
   {D,{Hour, Minute, Second, _Ms}} = to_date(Ts),
   qdate:to_string("Y-m-d h:ia", {D,{Hour, Minute, Second}}).

-spec millisecond(non_neg_integer()) -> non_neg_integer().
millisecond(Ts) ->
   faxe_time:get(millisecond, Ts).

-spec second(non_neg_integer()) -> non_neg_integer().
second(Ts) ->
   faxe_time:get(second, Ts).

-spec minute(non_neg_integer()) -> non_neg_integer().
minute(Ts) ->
   faxe_time:get(minute, Ts).

-spec hour(non_neg_integer()) -> non_neg_integer().
hour(Ts) ->
   faxe_time:get(hour, Ts).

-spec day_of_week(non_neg_integer()) -> non_neg_integer().
day_of_week(Ts) ->
   faxe_time:get(day_of_week, Ts).

%%day_of_week_string(Ts) ->
%%   list_to_binary(faxe_time:get(day_of_week_string, Ts)).

-spec week(non_neg_integer()) -> non_neg_integer().
week(Ts) ->
   faxe_time:get(week, Ts).

-spec month(non_neg_integer()) -> non_neg_integer().
month(Ts) ->
   faxe_time:get(month, Ts).
%%month_string(Ts) ->
%%   list_to_binary(faxe_time:get(month_string, Ts)).

-spec year(non_neg_integer()) -> non_neg_integer().
year(Ts) ->
   faxe_time:get(year, Ts).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% other functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

