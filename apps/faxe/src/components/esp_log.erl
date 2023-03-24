%% Date: 30.12.16 - 23:01
%% Log everything that comes in to a file, line by line
%% Ⓒ 2019 heyoka
%%
-module(esp_log).
-author("Alexander Minichmair").

-include("faxe.hrl").

-behavior(df_component).
%% API
-export([init/3, process/3, options/0, check_options/0]).

-record(state, {
   file  :: list(),
   field :: undefined | list(),
   format :: binary() %% <<"json">> | <<"raw">>
}).

options() ->
   [
      {file, string},
      {format, string, <<"json">>},
      {field, string, undefined}
      ].

check_options() ->
   [{not_empty, [file]}].

init(_NodeId, _Inputs, #{file := File, format := Format0, field := Field}) ->
   ok = filelib:ensure_dir(File),
   {ok, F} = file:open(File, [append, delayed_write]),
   Format = case Field of undefined -> Format0; _ -> <<"raw">> end,
   {ok, all, #state{file = F, field = Field, format = Format}}.

process(_In, P, State = #state{file = F, field = Field}) ->
   log(P, F, Field),
   {emit, P, State}.
%%process(_In, B = #data_batch{points = Ps}, State = #state{file = F, field = Field}) ->
%%   [log(P, F, Field) || P <- Ps],
%%   {emit, B, State}.

%% whole datapoint will be written as json
log(Point, File, undefined) ->
   Data = flowdata:to_json(Point),
   do_log(Data, File);
%% single field value will be written raw
log(Point, File, Field) ->
   Data0 = flowdata:field(Point, Field),
   Data =
   case Data0 of
      _D when is_map(Data0) -> jiffy:encode(Data0);
      _ -> Data0
   end,
   do_log(Data, File).

do_log(undefined, _File) ->
   ok;
do_log(Data, File) when is_binary(Data) ->
   do_log(binary_to_list(Data), File);
do_log(Data, File) when is_integer(Data) ->
   do_log(integer_to_list(Data), File);
do_log(Data, File) when is_float(Data) ->
   do_log(float_to_list(Data), File);
do_log(Data, File) when is_list(Data) ->
   write(Data, File);
do_log(_Data, _File) ->
   lager:notice("data is : ~p",[_Data]),
   ok.

write(Data, File) ->
   io:format(File, "~s~n", [Data]).