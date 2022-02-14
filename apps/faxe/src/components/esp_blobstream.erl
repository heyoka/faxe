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
   tried = 1 :: non_neg_integer(),
   in_progress = false :: true|false,
   waiting = [] :: list()

}).

%% python funs
-define(PYTHON_PREPARE_CALL, prepare).
-define(PYTHON_MODULE, azblobstream).
%% no "start" call here, because we cast a message to the python runtime, see handle_info(startstream, ....

-define(FORMAT_CSV, <<"csv">>).
-define(FORMAT_JSON, <<"json">>).

-define(META_FIELD, <<"meta">>).
-define(DATA_FIELD, <<"data">>).

-define(META_CHUNK, <<"chunk">>).
-define(META_LINE, <<"line">>).

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
   {chunk_size, integer, 8192},
   {format, string, ?FORMAT_CSV},
   {header_row, integer, 1},
   {data_start_row, integer, 2},
   {date_field, string, <<"date">>},
   {date_format, string, <<"Y-m-D H:M:s">>},
   {line_separator, string, <<"\n">>},
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

   State = #state{
      flow_inputs = Inputs,
      format = Format,
      dt_field = DtField,
      dt_format = DtFormat,
      node_id = NodeId,
      python_args = Args,
      initial_args = Args#{<<"erl">> => self()},
      options_path = OptField,
      max_retries = Tries},

   {ok, all, State}.


process(_Inp, #data_point{} = Point, State = #state{in_progress = true, waiting = List}) ->
   lager:warning("incoming point when in progress"),
   {ok, State#state{waiting = List ++ [Point]}};
process(_Inp, #data_point{} = Point, State = #state{}) ->
   {ok, process_point(Point, State)}.

process_point(#data_point{} = Point, State = #state{initial_args = InitArgs, options_path = OPath}) ->
   Mapped = map_point_data(Point, OPath),
%%   lager:notice("mapped point data: ~p", [Mapped]),
   PythonArgs = #{<<"format">> := Format, <<"date_field">> := DtField, <<"date_format">> := DtFormat} =
      maps:merge(InitArgs, Mapped),
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

handle_info(startstream, State=#state{max_retries = Max, tried = Max, item = Point}) ->
   lager:warning("max reties ~p reached we are done here",[Max]),
   dataflow:ack(Point, State#state.flow_inputs),
   NewState = reset_state(State),
   {ok, next(NewState)};
handle_info(startstream, State=#state{python_args = Args, tried = Tried}) ->
   State0 = setup_python(State),
   NewState =
   case pythra:pythra_call(State0#state.python_instance, ?PYTHON_MODULE, ?PYTHON_PREPARE_CALL, [Args]) of
      true ->
         pythra:cast(State0#state.python_instance, <<"let's go!">>),
         State0;
      false ->
         erlang:send_after(1000, self(), startstream),
         stop_python(State0)
   end,
   {ok, NewState#state{tried = Tried + 1}};
%% python sends us data
%% skip header for now
handle_info({emit_data, #{<<"header">> := _H}}, State=#state{}) ->
   lager:info("header: ~p", [_H]),
   {ok, State};
%% python is done with the current download
handle_info({emit_data, #{<<"done">> := _True}}, State=#state{item = Point}) ->
   lager:info("we are DONE with this file, stop the port to python"),
   %% lets ack to upstream nodes (dataflow acknowledge)
   dataflow:ack(Point, State#state.flow_inputs),
   NewState = reset_state(stop_python(State)),
   {ok, next(NewState)};
handle_info({emit_data, Data0}, State=#state{}) when is_map(Data0) ->
   {Chunk, Line, Point} = convert_data(Data0, State),
   {emit, {1, Point}, State#state{current_chunk = Chunk, current_line = Line}};
handle_info({emit_data, Data}, State=#state{}) when is_list(Data) ->
%%   lager:info("got data list from python: ~p",[Data]),
   Fun =
      fun(P, Acc=#{points := PointList}) ->
         {Ch, L, NewP} = convert_data(P, State),
         Acc#{points => PointList ++ [NewP], chunk => Ch, line => L}
      end,
   #{points := ResPoints, chunk := CChunk, line := CLine} = lists:foldl(Fun, #{points => []}, Data),
   Batch = #data_batch{points = ResPoints},
   {emit, {1, Batch}, State#state{current_chunk = CChunk, current_line = CLine}};
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
handle_info({'EXIT', _Who, normal}, State = #state{python_instance = _Py}) ->
   return_exit(State);
handle_info({'EXIT', Who, Reason}, State = #state{python_instance = Py, in_progress = true}) ->
   lager:warning("Python exited (~p / ~p), reason: ~p",[Who, Py, Reason]),
   %% retry
   erlang:send_after(1000, self(), startstream),
   return_exit(State);
handle_info(_Request, State) ->
   lager:notice("got from python: ~p", [_Request]),
   {ok, State}.

shutdown(S=#state{}) ->
   stop_python(S).

%%%%%%%%%%%%%%%%%%%% internal %%%%%%%%%%%%
next(State=#state{waiting = [Next|List]}) ->
   process_point(Next, State#state{waiting = List});
next(State=#state{waiting = []}) ->
   State.

return_exit(State=#state{}) ->
   {ok,
      State#state{
         python_instance = undefined
      }}.

reset_state(State=#state{}) ->
   State#state{
      python_instance = undefined,
      tried = 1,
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
   maps:fold(Fold, #{}, PointData).


convert_data(DataMap, S=#state{format = ?FORMAT_JSON, dt_field = DtField}) ->
   build_point(DataMap, DtField, S);
convert_data(DataMap, S=#state{format = ?FORMAT_CSV, dt_field = DtField}) ->
   DateTimeField = <<?DATA_FIELD/binary, ".", DtField/binary>>,
   {C, L, NewPoint0} = build_point(DataMap, DateTimeField, S),
%%   lager:notice("flowdata to  num: ~p",[lager:pr(flowdata:to_num(NewPoint), ?MODULE)]),
   NewPoint = flowdata:delete_field(NewPoint0, DateTimeField),
   {C, L, flowdata:to_num(NewPoint, ?DATA_FIELD)}.

build_point(DataMap, DateTimeField, #state{dt_format = DtFormat, python_args = PMeta}) ->
   Meta0 = maps:get(?META_FIELD, DataMap, #{}),
   Meta = #{?META_CHUNK := CChunk, ?META_LINE := CLine} = maps:merge(Meta0, PMeta),
%%   DtPath = <<?DATA_FIELD/binary, ".", DtField/binary>>,
   P = flowdata:point_from_json_map(DataMap#{?META_FIELD => Meta}, DateTimeField, DtFormat),
%%   lager:info("~p",[faxe_time:to_iso8601(flowdata:ts(POut))]),
   {CChunk, CLine, P}.

setup_python(State = #state{}) ->
   PInstance = get_python(),
   State#state{python_instance = PInstance}.%, cb_object = ClassInstance}.

stop_python(State = #state{python_instance = Py}) ->
   catch pythra:stop(Py),
   State#state{python_instance = undefined}.

get_python() ->
   {ok, PythonParams} = application:get_env(faxe, python),
   Path = proplists:get_value(script_path, PythonParams, "./python"),
   FaxePath = filename:join(code:priv_dir(faxe), "python/"),
   {ok, Python} = pythra:start_link([FaxePath, Path]),
   Python.


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