-module(bcd).
-compile(nowarn_export_all).
-compile(export_all).
% Binary Coded Decimal format


% pack the digits of an integer as BCD in a given size of binary
% pad with leading zeros
encode(N, Size) ->
  encode(N, Size, []).

encode(_, 0, Acc) -> list_to_binary(Acc);
encode(N, Size, Acc) ->
  B  = N  rem 10,
  N1 = N  div 10,
  A  = N1 rem 10,
  N2 = N1 div 10,
  encode(N2, Size - 1, [(A bsl 4) + B | Acc]).

% unpack the given size of BCD binary into an integer
% strip leading zeros
decode(N, Size) when size(N) =:= Size ->
  decode(N).

decode(<<>>) -> 0;
decode(<<0,Rest/binary>>) -> decode(Rest);
decode(<<0:4,X:4,Rest/binary>>) ->
  [$0 + X | decode_(Rest)];
decode(N) -> decode_(N).

decode_(N) ->
  [ $0+X ||  <<X:4>> <= N ].