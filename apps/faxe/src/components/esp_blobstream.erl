%%% Date: 21.01.22 - 13:11
%%% Ⓒ 2022 heyoka
%%% @doc
%%% stream data from a blob storage (azure blob storage is currently the only option)
%%% the blob is expected to be a comma separated file (CSV) or a text file with one json string per line
%%%
%%% this node can be fed with a data_point to change options and start a file stream
%%% @todo memoize chunk and line number, in case python fails in the middle of processing

-module(esp_blobstream).
-author("Alexander Minichmair").

-include("faxe.hrl").
%% API
-behavior(df_component).
%% API
-export([
   init/3, process/3,
   handle_info/2, options/0,
   get_python/0,
   shutdown/1, decode_from_python/1]).

-callback execute(tuple(), term()) -> tuple().

-record(state, {
   node_id :: binary(),
   flow_inputs = [] :: list(),
   python_instance :: pid()|undefined,
   python_args :: map(),
   initial_args :: map(),
   as = <<"data">> :: binary(),
   dt_field :: binary(),
   dt_format :: binary(),
   format :: binary(),
   item :: #data_point{},
   current_chunk :: non_neg_integer(),
   current_line :: non_neg_integer(),
   options_path :: undefined|binary(),
   max_retries :: non_neg_integer(),
   tried = 0 :: non_neg_integer(),
   in_progress = false :: true|false,
   waiting = [] :: list(),
   python_timeout :: undefined|reference(),
   blobs_seen = #mem_queue{}

}).

%% python funs
-define(PYTHON_PREPARE_CALL, prepare).
-define(PYTHON_MODULE, azblobstreampd).
%% no "start" call here, because we cast a message to the python runtime, see handle_info(startstream, ....

-define(FORMAT_CSV, <<"csv">>).
-define(FORMAT_JSON, <<"json">>).

-define(META_FIELD, <<"meta">>).
-define(DATA_FIELD, <<"data">>).

-define(META_CHUNK, <<"chunk">>).
-define(META_LINE, <<"line">>).

-define(PYTHON_TIMEOUT, 3*60*1000).

-spec options() -> list(
   {atom(), df_types:option_name()} |
   {atom(), df_types:option_name(), df_types:option_value()}
).
options() -> [
   {account_url, string, {azure_blob, account_url}},
   {az_sec, string, {azure_blob, account_secret}},
   {container, string, <<"test">>},
   {blob_name, string, <<"4ed182c6eb9e">>},
   {encoding, string, <<"utf-8">>},
   {chunk_size, integer, 4096},
   {format, string, ?FORMAT_CSV},
   {header_row, integer, 1},
   {data_start_row, integer, 2},
   {date_field, string, <<"date">>},
   {date_format, string, <<"Y-m-D H:M:s">>},
%%   {line_separator, string, <<"\n">>},
   {column_separator, string, <<",">>},
   {batch_size, integer, 120},

   %% cannot be set with data_point values
   {opts_field, string, undefined},
   {retries, integer, 3}
].

init(NodeId, Inputs,
    #{date_field := DtField, date_format := DtFormat, format := Format, opts_field := OptField, retries := Tries}
       = Args0) ->

   Args = maps:fold(fun(K, V, Acc) -> Acc#{atom_to_binary(K) => V} end, #{}, Args0),
   process_flag(trap_exit, true),

   PArgs = maps:without([<<"_name">>, <<"retries">>, <<"opts_field">>], Args),

   State = #state{
      flow_inputs = Inputs,
      format = Format,
      dt_field = DtField,
      dt_format = DtFormat,
      node_id = NodeId,
      python_args = PArgs,
      initial_args = PArgs#{<<"erl">> => self()},
      options_path = OptField,
      max_retries = Tries,
      blobs_seen = memory_queue:new(15)},

   {ok, all, State}.


process(_Inp, #data_point{} = Point, State = #state{in_progress = true, waiting = List}) ->
   lager:warning("incoming point when in progress"),

   NewState =
   case blob_seen(Point, State) of
      true ->
         State;
      false ->
         State#state{waiting = List ++ [Point]}
   end,
   {ok, NewState};
process(_Inp, #data_point{} = Point, State = #state{}) ->
   NewState =
   case blob_seen(Point, State) of
      true ->
         State;
      false ->
         process_point(Point, State)
   end,
   {ok, NewState}.

process_point(#data_point{} = Point, State = #state{initial_args = InitArgs, options_path = OPath}) ->
   Mapped = map_point_data(Point, OPath),

%%   lager:notice("mapped point data: ~p", [Mapped]),
   PythonArgs = #{<<"format">> := Format, <<"date_field">> := DtField, <<"date_format">> := DtFormat} =
      maps:merge(InitArgs, Mapped),
   lager:notice("Process demand: ~p",[maps:without([<<"az_sec">>, <<"broker">>, <<"url">>], PythonArgs)]),
%%   lager:notice("new python args: ~p~nold:~p",[PythonArgs, InitArgs]),
   erlang:send_after(0, self(), startstream),
   State#state{
      item = Point,
      dt_format = DtFormat,
      dt_field = DtField,
      format = Format,
      python_args = PythonArgs,
      in_progress = true
   }.

handle_info(startstream, State=#state{max_retries = Max, tried = Max, item = Point, python_args = Args}) ->
   lager:warning("max retries ~p reached, flow-ack Point (~p)",[Max, blob_name(Args)]),
   dataflow:ack(Point, State#state.flow_inputs),
   NewState = reset_state(State),
   {ok, next(NewState)};
handle_info(startstream, State=#state{python_args = Args, tried = Tried, blobs_seen = Mem}) ->
   BlobName = blob_name(Args),
   State0 = setup_python(State),
   lager:info("prepare download ~p",[BlobName]),
   NewState =
   case pythra:pythra_call(State0#state.python_instance, ?PYTHON_MODULE, ?PYTHON_PREPARE_CALL, [Args]) of
      true ->
         lager:notice("start downloading ~p (try: ~p)",[BlobName, Tried+1]),
         pythra:cast(State0#state.python_instance, <<"go">>),
         SeenList = memory_queue:enq(BlobName, Mem),
%%         lager:notice("seen sofar: ~p",[SeenList]),
         %% call timeout
         start_p_timeout(State0#state{blobs_seen = SeenList});
      false ->
         erlang:send_after(300, self(), startstream),
         stop_python(State0)
   end,
   {ok, NewState#state{tried = Tried + 1}};
%% python sends us data
%% skip header for now
handle_info(ptimeout, State=#state{}) ->
   lager:notice("timeout python (~p ms), try restarting", [?PYTHON_TIMEOUT]),
   erlang:send_after(0, self(), startstream),
   {ok, State};
handle_info({emit_data, #{<<"header">> := _H}}, State=#state{}) ->
   lager:info("header: ~p", [_H]),
   {ok, State};
%% python is done with the current download
handle_info({emit_data, #{<<"done">> := _True}}, State=#state{item = Point, python_args = Args}) ->
   lager:notice("DONE downloading file ~p, emitted ~p lines",
      [blob_name(Args), State#state.current_line]),
   %% lets ack to upstream nodes (dataflow acknowledge)
   dataflow:ack(Point, State#state.flow_inputs),
   NewState = reset_state(stop_python(State)),
   {ok, next(NewState)};
handle_info({emit_data, Data0}, State=#state{}) when is_map(Data0) ->
   {Chunk, Line, Point} = convert_data(Data0, State),
   {emit, {1, Point}, State#state{current_chunk = Chunk, current_line = Line}};
handle_info({emit_data, Data}, State=#state{python_args = Args}) when is_list(Data) ->
%%   lager:info("got data list from python: ~p",[Data]),
   Fun =
      fun(P, Acc=#{points := PointList}) ->
%%         lager:notice("from python: ~p",[P]),
         {Ch, L, NewP} = convert_data(P, State),
         Acc#{points => PointList ++ [NewP], chunk => Ch, line => L}
      end,
   case catch lists:foldl(Fun, #{points => []}, Data) of
      #{points := ResPoints, chunk := CChunk, line := CLine} ->
         Batch = #data_batch{points = ResPoints},
         {emit, {1, Batch}, State#state{current_chunk = CChunk, current_line = CLine}};
      Err -> lager:warning("Error converting data_batch for blob ~p batch dropped, Reason: ~p",[blob_name(Args), Err]),
         {ok, State}
   end;

handle_info({emit_data, {"Map", Data}}, State) when is_list(Data) ->
%%   lager:info("python data: ~p",[Data]),
%%   lager:notice("got point data from python: ~p", [Data]),
   {emit, {1, Data}, State};
handle_info({python_error, ErrBin}, State) ->
   lager:error("error from python: ~s", [ErrBin]),
   {ok, stop_python(State)};
handle_info({'EXIT', _P,
   {message_handler_error, {python, 'azure.core.exceptions.ResourceNotFoundError', ErrorMsg,
      {'$erlport.opaque',python, _Bin} }}}, State = #state{}) ->
   lager:warning("ResourceNotFoundError: ~p",[ErrorMsg]),
   return_exit(State);
handle_info({'EXIT', _P,
   {message_handler_error, {python, 'azure.core.exceptions.ClientAuthenticationError', ErrorMsg,
      {'$erlport.opaque',python, _Bin} }}}, State = #state{}) ->
   lager:warning("ClientAuthenticationError: ~p",[ErrorMsg]),
   return_exit(State);
handle_info({'EXIT', Who, normal}, State = #state{python_instance = Py}) ->
   lager:info("EXIT normal (~p/~p)",[Who, Py]),
   {ok, State};
handle_info({'EXIT', Who, Reason}, State = #state{python_instance = Py, in_progress = true}) ->
   lager:warning("Python exited (~p / ~p), reason: ~p",[Who, Py, Reason]),
   %% retry
   erlang:send_after(100, self(), startstream),
   return_exit(State);
handle_info(_Request, State) ->
   lager:notice("got unknown: ~p", [_Request]),
   {ok, State}.

shutdown(S=#state{}) ->
   stop_python(S).

%%%%%%%%%%%%%%%%%%%% internal %%%%%%%%%%%%
blob_name(#{<<"blob_name">> := B}) ->
   B;
blob_name(_) ->
   <<"unknown_blob">>.

blob_seen(P = #data_point{}, State = #state{blobs_seen = Seen}) ->
   BlobName = flowdata:field(P, <<"blob_name">>),
   case memory_queue:member(BlobName, Seen) of
      true ->
         lager:notice("blob ~p already seen before",[BlobName]),
         %% ack here, we have seen this blob before
         dataflow:ack(P, State#state.flow_inputs),
         true;
      false -> false
   end.


next(State=#state{waiting = [Next|List]}) ->
   process_point(Next, State#state{waiting = List});
next(State=#state{waiting = []}) ->
   State.

return_exit(State=#state{}) ->
   {ok,
      cancel_p_timeout(
         State#state{
            python_instance = undefined
         }
      )
   }.

reset_state(State=#state{}) ->
   State#state{
      python_instance = undefined,
      tried = 0,
      current_chunk = 0,
      current_line = 0,
      in_progress = false
   }.

map_point_data(#data_point{fields = FieldMap, ts = Ts}, undefined) ->
   map_point_data(FieldMap#{ts => Ts});
map_point_data(#data_point{ts = Ts} = P, OptionsPath) ->
   Map = flowdata:field(P, OptionsPath),
   map_point_data(Map#{ts => Ts}).
map_point_data(PointData) ->
   Fold =
      fun(K, V, Acc) ->
         case is_binary(V) orelse is_number(V) of
            true ->
               NewKey =
               case is_atom(K) of
                  true -> atom_to_binary(K);
                  false -> K
               end,
               Acc#{NewKey => V};
            false -> Acc
         end
      end,
   maps:fold(Fold, #{}, maps:without([<<"broker">>, <<"url">>],PointData)).


convert_data(DataMap, S=#state{format = ?FORMAT_JSON, dt_field = DtField}) ->
   {C, L, P=#data_point{fields = #{?META_FIELD := Meta}=Fields}} = build_point(DataMap, DtField, S),
   DataPoint=#data_point{fields = NFields} =
      flowdata:set_root(P#data_point{fields = maps:without([?META_FIELD], Fields)}, ?DATA_FIELD),
   DP = DataPoint#data_point{fields = NFields#{?META_FIELD=>Meta}},
   {C, L, DP};
convert_data(DataMap, S=#state{format = ?FORMAT_CSV, dt_field = DtField}) ->
   DateTimeField = <<?DATA_FIELD/binary, ".", DtField/binary>>,
   {C, L, NewPoint} = build_point(DataMap, DateTimeField, S),
%%   lager:notice("flowdata to  num: ~p",[lager:pr(flowdata:to_num(NewPoint), ?MODULE)]),
   {C, L, flowdata:to_num(NewPoint, ?DATA_FIELD)}.

build_point(DataMap = #{?META_FIELD := #{?META_CHUNK := CChunk, ?META_LINE := CLine}},
    DateTimeField, #state{dt_format = DtFormat}) ->
   P = flowdata:point_from_json_map(DataMap , DateTimeField, DtFormat),
%%   lager:info("chunk: ~p, line: ~p",[CChunk, CLine]),
   {CChunk, CLine, flowdata:delete_field(P, DateTimeField)}.


setup_python(State = #state{}) ->
   PInstance = get_python(),
   State#state{python_instance = PInstance}.

stop_python(State = #state{python_instance = Py}) ->
   catch pythra:stop(Py),
   NewState = cancel_p_timeout(State),
   NewState#state{python_instance = undefined}.

get_python() ->
   {ok, PythonParams} = application:get_env(faxe, python),
   Path = proplists:get_value(script_path, PythonParams, "./python"),
   FaxePath = filename:join(code:priv_dir(faxe), "python/"),
   {ok, Python} = pythra:start_link([FaxePath, Path]),
   Python.

start_p_timeout(State = #state{}) ->
   lager:info("start python timeout"),
   TRef = erlang:send_after(?PYTHON_TIMEOUT, self(), ptimeout),
   State#state{python_timeout = TRef}.

cancel_p_timeout(State = #state{python_timeout = undefined}) ->
   State;
cancel_p_timeout(State = #state{python_timeout = TRef}) ->
   lager:info("stop python timeout"),
   catch erlang:cancel_timer(TRef),
   State#state{python_timeout = undefined}.

decode_from_python(Map) ->
   Dec = fun(K, V, NewMap) ->
      NewKey =
      case K of
         _ when is_binary(K) -> K;
         _ when is_list(K) -> list_to_binary(K)
      end,
      NewValue =
      case V of
         _ when is_map(V) -> decode_from_python(V);
         _ when is_list(V) -> list_to_binary(V);
         _ -> V
      end,
      NewMap#{NewKey => NewValue}
         end,
   maps:fold(Dec, #{}, Map).

-ifdef(TEST).

test_meta() ->
   #{?META_CHUNK => 14, ?META_LINE => 156}.

convert_json_1_test() ->
   Map0 = #{<<"data">> => #{
      <<"add">> => <<"/FlashLoop/World15">>,
      <<"dur">> => 5050.0,
      <<"ts">> => <<"2021-11-17T16:08:48.527000Z">>}},
   Map = maps:merge(#{?META_FIELD => test_meta()}, Map0),
   {14, 156, P} =
      convert_data(Map, #state{format = ?FORMAT_JSON, dt_field = <<"data.ts">>, dt_format = <<"ISO8601">>}),
   Expected = #data_point{ts=1637165328527,
      fields = #{
         ?META_FIELD => test_meta(),
         ?DATA_FIELD => #{
            <<"add">> => <<"/FlashLoop/World15">>,
            <<"dur">> => 5050.0}
      }
   },
   ?assertEqual(Expected, P).

convert_json_2_test() ->
   Map0 = #{
      <<"add">> => <<"/FlashLoop/World15">>,
      <<"dur">> => 5050.0,
      <<"datetime">> => <<"1637165328527">>},
   Map = maps:merge(#{?META_FIELD => test_meta()}, Map0),
   {14, 156, P} =
      convert_data(Map, #state{format = ?FORMAT_JSON, dt_field = <<"datetime">>, dt_format = <<"millisecond">>}),
   Expected = #data_point{ts=1637165328527,
      fields = #{
         ?META_FIELD => test_meta(),
         ?DATA_FIELD => #{
            <<"add">> => <<"/FlashLoop/World15">>,
            <<"dur">> => 5050.0}
      }
   },
   ?assertEqual(Expected, P).

convert_json_3_test() ->
   Map0 = #{<<"data">> => #{
      <<"add">> => <<"/FlashLoop/World15">>,
      <<"dur">> => <<"5050">>},
      <<"datetime">> => <<"1637165328527">>},
   Map = maps:merge(#{?META_FIELD => test_meta()}, Map0),
   {14, 156, P} =
      convert_data(Map, #state{format = ?FORMAT_JSON, dt_field = <<"datetime">>, dt_format = <<"millisecond">>}),
   Expected = #data_point{ts=1637165328527,
      fields = #{
         ?META_FIELD => test_meta(),
         ?DATA_FIELD => #{
            <<"add">> => <<"/FlashLoop/World15">>,
            <<"dur">> => <<"5050">>}
      }
   },
   ?assertEqual(Expected, P).

convert_csv_1_test() ->
   Map0 = #{<<"data">> => #{
      <<"add">> => <<"/FlashLoop/World15">>,
      <<"dur">> => 5050.0,
      <<"date">> => <<"2021-11-17T16:08:48.527000Z">>}} ,
   Map = maps:merge(#{?META_FIELD => test_meta()}, Map0),
   {14, 156, P} =
      convert_data(Map, #state{format = ?FORMAT_CSV, dt_field = <<"date">>, dt_format = <<"ISO8601">>}),
   Expected = #data_point{ts = 1637165328527,
      fields = #{
         ?META_FIELD => test_meta(),
         ?DATA_FIELD => #{
            <<"add">> => <<"/FlashLoop/World15">>,
            <<"dur">> => 5050.0}
      }
   },
   ?assertEqual(Expected, P).

convert_csv_2_test() ->
   Map0 = #{<<"data">> => #{
      <<"add">> => <<"/FlashLoop/World15">>,
      <<"dur">> => <<"5050.0">>,
      <<"date">> => <<"2021-11-17T16:08:48.527000Z">>}} ,
   Map = maps:merge(#{?META_FIELD => test_meta()}, Map0),
   {14, 156, P} =
      convert_data(Map, #state{format = ?FORMAT_CSV, dt_field = <<"date">>, dt_format = <<"ISO8601">>}),
   Expected = #data_point{ts = 1637165328527,
      fields = #{
         ?META_FIELD => test_meta(),
         ?DATA_FIELD => #{
            <<"add">> => <<"/FlashLoop/World15">>,
            <<"dur">> => 5050.0}
      }
   },
   ?assertEqual(Expected, P).


convert_csv_3_test() ->
   qdate:start(),
   Map0 = #{<<"data">> => #{
      <<"add">> => <<"/FlashLoop/World15">>,
      <<"dur">> => <<"5050.0">>,
      <<"date">> => <<"7/18/2019 5:08:43.064000000 PM">>}} ,
   Map = maps:merge(#{?META_FIELD => test_meta()}, Map0),
   {14, 156, P} =
      convert_data(Map, #state{format = ?FORMAT_CSV, dt_field = <<"date">>, dt_format = <<"n/d/Y l:M:S.f p">>}),
   Expected = #data_point{ts = 1563469723064,
      fields = #{
         ?META_FIELD => test_meta(),
         ?DATA_FIELD => #{
            <<"add">> => <<"/FlashLoop/World15">>,
            <<"dur">> => 5050.0}
      }
   },
   ?assertEqual(Expected, P).

-endif.