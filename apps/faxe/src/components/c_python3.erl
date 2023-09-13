%%% Date: 10.11.22 - 14:11
%%% Ⓒ 2022 heyoka
%%%
%%% @doc
%%% version 2 of faxe's python interface, has a more streamlined data interface
%%% supporting complete data-point and data-batch python representations
%%% in python data items are still dicts

-module(c_python3).
-author("Alexander Minichmair").

-include("faxe.hrl").
%% API
-behavior(df_component).
%% API
-export([
   init/3, process/3,
   handle_info/2, options/0,
   call_options/2, get_python/1,
   shutdown/1, get_stats/1]).

-callback execute(tuple(), term()) -> tuple().

-record(state, {
   node_id :: binary(),
   callback_module :: atom(),
   callback_class :: atom(),
   python_instance :: pid()|undefined,
   python_args :: map(),
   cb_object :: term(),
   func_calls = [],
   as :: binary()|undefined,
   stop_on_exit :: boolean()
}).


%% python method calls
-define(PYTHON_INFO, info).
-define(PYTHON_INIT, <<"init">>).
-define(PYTHON_BATCH, <<"batch">>).
-define(PYTHON_POINT, <<"point">>).

-define(BATCH_CHUNK_SIZE, 350).


add_options() ->
   [
      {as, string, undefined},
      %% stop node (and therefore flow on exit of python instance)
      {stop_on_exit, boolean, true}
   ].

-spec options() -> list(
   {atom(), df_types:option_name()} |
   {atom(), df_types:option_name(), df_types:option_value()}
).
options() ->
   [
      {cb_module, atom},
      {cb_class, atom}
   ]
   ++ add_options().



%% @doc get the options to recognize in dfs for the python node (from the callback class)
-spec call_options(atom(), atom()) -> list(tuple()).
call_options(Module, Class) ->

   process_flag(trap_exit, true),
   P = get_python(Class),
   ModClass = list_to_atom(atom_to_list(Module)++"."++atom_to_list(Class)),
%%   lager:notice("call options ~p",[{Module, Class, ModClass}]),
   Res =
      try pythra:func(P, ModClass, ?PYTHON_INFO, [Class]) of
         B when is_list(B) -> add_options() ++ B
      catch
         _:{python,'builtins.ModuleNotFoundError', Reason,_}:_Stack ->
            Err = lists:flatten(io_lib:format("python module not found: ~s",[Reason])),
            {error, Err}
      end,
   python:stop(P),
   Res.


init(NodeId, _Ins, #{cb_module := Callback, cb_class := CBClass, as := As, stop_on_exit := StopOnExit} = Args) ->
   process_flag(trap_exit, true),
   PInstance = python_init(CBClass, Args),
   State = #state{
      callback_module = Callback,
      callback_class =  CBClass,
      node_id = NodeId,
      python_instance = PInstance,
      python_args = Args,
      as = As,
      stop_on_exit = StopOnExit},
   {ok, all, State}.

process(_Inp, #data_batch{} = Batch, State = #state{python_instance = Python}) ->
   Data =
      case jiffy:encode(to_map(Batch)) of
         JList when is_list(JList) -> iolist_to_binary(JList);
         Other -> Other
      end,
%%   lager:notice("send batch json to python with size: ~p", [length(Points)]),
   pythra:cast(Python, [?PYTHON_BATCH, Data]),
   {ok, State}
;
process(_Inp, #data_point{} = Point, State = #state{python_instance = Python}) ->
   Data = to_map(Point),
   pythra:cast(Python, [?PYTHON_POINT, Data]),
   {ok, State}.

get_stats(#state{python_instance = Python}) ->
   {_, ProcessStats} = pythra:pythra_call(Python, 'faxe_handler', 'process_stats'),
   ProcessStats.

%% python sends us data
handle_info({emit_data, #{<<"fields">> := Fs}= Data} , State = #state{as = As}) when is_map(Fs)->
   Point = flowdata:point_from_json_map(Data),
   {emit, {1, from_map(Point, As)}, State};
handle_info({emit_data, #{<<"points">> := Points}=BatchData}, State = #state{as = As}) ->
   NewPoints = [from_map(flowdata:point_from_json_map(P), As) || P <- Points],
   Batch0 = #data_batch{points = NewPoints},
   StartTs =
   case maps:is_key(<<"start_ts">>, BatchData) of
      true -> maps:get(<<"start_ts">>, BatchData);
      false -> flowdata:first_ts(Batch0)
   end,
   Batch = Batch0#data_batch{start = StartTs},
   {emit, {1, Batch}, State};
%% json data from python will be transferred as a string
handle_info({emit_data, Data}, State = #state{}) when is_list(Data) ->
   BatchData = jiffy:decode(Data, [return_maps, {null_term, undefined}]),
   handle_info({emit_data, BatchData}, State);
handle_info({python_error, Error}, State) ->
   lager:error("~p", [Error]),
   {ok, State};
handle_info({python_log, Message, Level0} = _L, State) ->
   Level = log_level(faxe_util:to_bin(Level0)),
   esp_debug:do_log(Level, "~p", [Message]),
   {ok, State};
%%%
handle_info(start_debug, State) ->
   {ok, State};
handle_info({'EXIT', Python,
   {message_handler_error, {python, PErrName, ErrorMsg, {'$erlport.opaque',python, _Bin} }}},
      State = #state{python_instance = Python}) ->

   lager:warning("Python exited with: ~p",[{PErrName, ErrorMsg}]),
   handle_exit(PErrName, State);
%%   erlang:send_after(1000, self(), restart_python),
%%   {ok, State#state{python_instance = undefined}};
handle_info(restart_python, State = #state{python_args = Args, callback_class = CBClass}) ->
   PInstance = python_init(CBClass, Args),
   {ok, State#state{python_instance = PInstance}};
handle_info(_Request, State) ->
%%   lager:notice("got unexpected: ~p", [Request]),
   {ok, State}.

shutdown(#state{python_instance = Python}) ->
   pythra:stop(Python).

%%%%%%%%%%%%%%%%%%%% internal %%%%%%%%%%%%
to_map(#data_point{ts = Ts, fields = Fields, tags = Tags, dtag = DTag}) ->
   #{
      <<"ts">> => Ts,
      <<"fields">> => Fields,
      <<"tags">> => Tags,
      <<"dtag">> => DTag
   };
to_map(B=#data_batch{start = undefined}) ->
   batch_to_map(B#data_batch{start = flowdata:first_ts(B)});
to_map(B=#data_batch{}) ->
   batch_to_map(B).
batch_to_map(#data_batch{points = Points, start = Start, dtag = DTag}) ->
   #{
      <<"start_ts">> => Start,
      <<"points">> => [to_map(P) || P <- Points],
      <<"dtag">> => DTag
   }.

from_map(P = #data_point{fields = Fields}, As) when is_map_key(<<"fields">>, Fields) ->
   NewFields = maps:get(<<"fields">>, Fields),
   P1 = P#data_point{fields = maps:without([<<"dtag">>, <<"fields">>, <<"tags">>, <<"ts">>], NewFields)},
   flowdata:set_root(P1, As);
from_map(P = #data_point{fields = Fields}, As) ->
   P1 = P#data_point{fields = maps:without([<<"dtag">>, <<"tags">>, <<"ts">>], Fields)},
   flowdata:set_root(P1, As).


get_python(CBClass) ->
   {ok, PythonParams} = application:get_env(faxe, python),
   Path = proplists:get_value(script_path, PythonParams, "./python"),
   FaxePath = filename:join(code:priv_dir(faxe), "python/"),
   {ok, Python} = pythra:start_link([FaxePath, Path]),
%%   lager:notice("python ~p",[Python]),
   ok = pythra:func(Python, faxe_handler, register_handler, [CBClass]),
   Python.

python_init(CBClass, Args) ->
   PInstance = get_python(CBClass),
   %% create an instance of the callback class
   PyOpts = maps:without([cb_module, cb_class], Args#{<<"erl">> => self()}),
   pythra:cast(PInstance, [?PYTHON_INIT, CBClass, PyOpts]),
   PInstance.

handle_exit(Reason, State = #state{stop_on_exit = true}) ->
   {stop, Reason, State};
handle_exit(_R, State) ->
   erlang:send_after(1000, self(), restart_python),
   {ok, State#state{python_instance = undefined}}.



log_level(<<"debug">>) -> debug;
log_level(<<"info">>) -> info;
log_level(<<"notice">>) -> notice;
log_level(<<"warning">>) -> warning;
log_level(<<"error">>) -> error;
log_level(<<"alert">>) -> alert;
log_level(_) -> notice.
