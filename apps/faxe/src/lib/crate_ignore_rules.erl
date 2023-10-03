%%%-------------------------------------------------------------------
%%% @author heyoka
%%% @copyright (C) 2023, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 25. Sep 2023 7:49 AM
%%%-------------------------------------------------------------------
-module(crate_ignore_rules).
-author("heyoka").

%% API
-export([check_ignore_error/2, init_rules/0, get_rules/0, add_rule/2]).


init_rules() ->
  Rules0 = faxe_util:to_bin(faxe_config:get_sub(crate, ignore_rules, <<>>)),
  Rules1 = binary:split(Rules0, <<",">>, [global, trim_all]),
  Rules = lists:map(fun(R) ->
    [RType, RValue] = binary:split(R, <<"=">>, [global, trim_all]),
    Value = case RType of <<"code">> -> binary_to_integer(RValue); _ -> RValue end,
    {RType, Value}
    end, Rules1),
  ets:insert(crate_ignore_rules, {rules, Rules}).

get_rules() ->
  case ets:lookup(crate_ignore_rules, rules) of
    [{rules, Rules}] -> Rules;
    _ -> []
  end.

add_rule(<<"code">> = T, Value) when is_binary(Value) ->
  add_rule(T, binary_to_integer(Value));
add_rule(Type, Value) ->
  Rules = get_rules(),
  NewRules = [{Type,Value}|Rules],
  ets:insert(crate_ignore_rules, {rules, NewRules}).

check_ignore_error(Code, Message) ->
  Rules = get_rules(),
  Check =
    fun
      ({<<"code">>, RuleCode}) -> RuleCode == Code;
      ({<<"message">>, MsgPart}) -> estr:str_contains(Message, MsgPart)
    end,
  case lists:any(Check, Rules) of
    true ->
      % ignore
      lager:notice("IGNORE server error because of rule ~p",[Rules]),
      {error, invalid};
    false ->
      {failed, server_error}
  end.