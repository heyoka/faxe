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
%% the module estr has several string manipulation functions, these can be used:

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
str_concat(Strings) when is_list(Strings) ->
   unicode:characters_to_binary(Strings).
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Math functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% all function from erlang's 'math' module can be used in lambda-expressions
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
%% pi() -> float()
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
%%   max/2
%%
%%
%%% @end

defined(Val) ->
   Val /= undefined.
undefined(Val) ->
   Val == undefined.

topic_part(Topic, Index) when is_binary(Topic), is_integer(Index) ->
   topic_part(Topic, Index, <<"/">>).
topic_part(Topic, Index, Separator) when is_binary(Topic), is_binary(Separator), is_integer(Index) ->
   Parts = binary:split(Topic, Separator, [global, trim_all]),
   case (catch lists:nth(Index, Parts)) of
      Result when is_binary(Result) -> Result;
      _E ->
         erlang:error("faxe_lambda_lib, topic_part", [Topic, Index, Separator])
   end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% additional
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
round_float(Int, _Precision) when is_integer(Int) ->
   Int;
round_float(Float, Precision) when is_float(Float), is_integer(Precision) ->
   faxe_util:round_float(Float, Precision).

max([]) -> 0;
max(ValueList) when is_list(ValueList) ->
   lists:max(ValueList).
min([]) -> 0;
min(ValueList) when is_list(ValueList) ->
   lists:min(ValueList).

%% modulo function
modulo(X, Y) ->
   mod(X, Y).
mod(X, Y) ->
   faxe_time:mod(X, Y).
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Time related functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% get the current ms timestamp UTC
-spec now() -> non_neg_integer().
now() ->
   faxe_time:now().

to_date(Ts) -> faxe_time:to_date(Ts).

-spec to_date_string(non_neg_integer()) -> string().
to_date_string(Ts) ->
   {D,{Hour, Minute, Second, _Ms}} = to_date(Ts),
   qdate:to_string("Y-m-d h:ia", {D,{Hour, Minute, Second}}).

-spec to_iso8601(non_neg_integer()) -> binary().
to_iso8601(Ts) -> faxe_time:to_iso8601(Ts).
-spec to_rfc3339(non_neg_integer()) -> binary().
to_rfc3339(Ts) -> time_format:to_rfc3339(Ts).

time_convert(Ts, Format) ->
   time_format:convert(Ts, Format).

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
-spec random(non_neg_integer()) -> non_neg_integer().
random(N) when is_integer(N), N > 0 ->
   rand:uniform(N).

%% @doc generate a random float between 0.0 and 1.0, that gets multiplied by N
-spec random_real(non_neg_integer()) -> float().
random_real(N) ->
   rand:uniform_real() * N.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% list/map functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec member(binary()|number(), list()|map()) -> true|false.
member(Ele, List) when is_list(List) -> lists:member(Ele, List);
member(Ele, Map) when is_map(Map) andalso is_map_key(Ele, Map) -> true;
member(_Ele, _) -> false.
-spec not_member(binary()|number(), list()|map()) -> true|false.
not_member(Ele, Coll) -> not member(Ele, Coll).

%%% maps
-spec map_get(binary(), map()) -> term().
map_get(Key, Map) -> maps:get(Key, Map, undefined).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% lambda state functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
ls_mem_list(Key) ->
   mem_lookup(Key).
ls_mem_set(Key) ->
   mem_lookup(Key).
ls_mem(Key) ->
   mem_lookup(Key).
mem_lookup(Key) ->
   Res =
   case ets:lookup(graph_node_registry:get_graph_table(), Key) of
      [{Key, Val}] -> Val;
      Other -> Other
   end,
   Res.