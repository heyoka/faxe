%% Date: 12.04.17 - 20:23
%% Ⓒ 2017 heyoka
%%
%% @doc
%% lambda function standard library
%%
-module(faxe_lambda_lib).
-author("Alexander Minichmair").

%% API
-compile(nowarn_export_all).
-compile(export_all).
%%% @doc

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% string functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% the module estr has several string manipulation function, that can be used:

%%    str_at/2
%%,   str_capitalize/1
%%,   str_chunk/2
%%,   str_codepoints/1
%%,   str_contains/2
%%,   str_downcase/1
%%,   str_ends_with/2
%%,   str_ends_with_any/2
%%,   str_eqi/2
%%,   str_first/1
%%,   str_last/1
%%,   str_length/1
%%,   str_lstrip/1
%%,   str_lstrip/2
%%,   str_next_codepoint/1
%%,   str_normalize/2
%%,   str_pad_leading/2
%%,   str_pad_leading/3
%%,   str_pad_trailing/2
%%,   str_pad_trailing/3
%%,   str_replace/3
%%,   str_replace_leading/3
%%,   str_replace_prefix/3
%%,   str_replace_suffix/3
%%,   str_replace_trailing/3
%%,   str_reverse/1
%%,   str_rstrip/1
%%,   str_rstrip/2
%%,   str_slice/3
%%,   str_split/1
%%,   str_split/2
%%,   str_split/3
%%,   str_split_at/2
%%,   str_split_by_any/2
%%,   str_split_by_any/3
%%,   str_split_by_re/2
%%,   str_split_by_re/3
%%,   str_starts_with/2
%%,   str_starts_with_any/2
%%,   str_strip/1
%%,   str_strip/2
%%,   str_upcase/1
%%%%%%%%%%% additional str funcs
str_concat(String1, String2) ->
   unicode:characters_to_binary([String1, String2]).
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Math functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% all function from the 'math' module can be used in lambda-expressions
%% these are:

%% acos(X) -> float()
%%
%% acosh(X) -> float()
%%
%% asin(X) -> float()
%%
%% asinh(X) -> float()
%%
%% atan(X) -> float()
%%
%% atan2(Y, X) -> float()
%%
%% atanh(X) -> float()
%%
%% ceil(X) -> float()
%%
%% cos(X) -> float()
%%
%% cosh(X) -> float()
%%
%% exp(X) -> float()
%%
%% floor(X) -> float()
%%
%% fmod(X, Y) -> float()
%%
%% log(X) -> float()
%%
%% log10(X) -> float()
%%
%% log2(X) -> float()
%%
%% pow(X, Y) -> float()
%%
%% sin(X) -> float()
%%
%% sinh(X) -> float()
%%
%% sqrt(X) -> float()
%%
%% tan(X) -> float()
%%
%% tanh(X) -> float()


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Mathex
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% all functions from the module mathex can be used
%% these are:

%% moving_average/1,
%% average/1,
%% sum/1,
%% stdev_sample/1,
%% stdev_population/1,
%% skew/1,
%% kurtosis/1,
%% variance/1,
%% covariance/2,
%% correlation/2,
%% correlation_matrix/1,
%% nth_root/2,
%% percentile/2,
%% zscore/1

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% std-lib
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% basic operators you can use in lambda expressions:
%% 'AND' -> " andalso ";
%% 'OR'  -> " orelse ";
%% '<='  -> " =< ";
%% '=>'  -> " >= ";
%% '!='  -> " /= ";
%% '!'   -> " not ";

%% dfs includes a std-lib, these functions are defined:

%%   type-conversions:
%%
%%   bool/1,
%%   int/1,
%%   float/1,
%%   string/1
%%
%%   some basic math funs:
%%
%%   abs/1,
%%   round/1,
%%   floor/1,
%%   min/2,
%%   max/2]
%%
%%
%%% @end
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% additional
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
round_float(Float, Precision) when is_float(Float), is_integer(Precision) ->
   list_to_float(io_lib:format("~.*f",[Precision, Float])).

max([]) -> 0;
max(ValueList) when is_list(ValueList) ->
   lists:max(ValueList).
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Time related functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% get the current ms timestamp UTC
now() ->
   faxe_time:now().

to_date(Ts) -> faxe_time:to_date(Ts).

to_date_string(Ts) ->
   {D,{Hour, Minute, Second, _Ms}} = to_date(Ts),
   qdate:to_string("Y-m-d h:ia", {D,{Hour, Minute, Second}}).

to_iso8601(Ts) -> faxe_time:to_iso8601(Ts).

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

-spec day(non_neg_integer()) -> non_neg_integer().
day(Ts) ->
   faxe_time:get(day, Ts).

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
%%% random generators
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc generate a random integer between 1 and N
random(N) when is_integer(N), N > 0 ->
   rand:uniform(N).

%% @doc generate a random float, that gets multiplied by N
random_real(N) ->
   rand:uniform_real() * N.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% list functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
member(Ele, List) -> lists:member(Ele, List).
not_member(Ele, List) -> lists:member(Ele, List) == false.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% lambda state functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
ls_mem(Name) ->
   Res =
   case ets:lookup(ls_mem, Name) of
      [{Name, Val}] -> Val;
      Other -> Other
   end,
%%   lager:info("ls_mem(~p) gives ~p",[Name, Res]),
   Res.
