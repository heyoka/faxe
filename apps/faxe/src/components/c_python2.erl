%%% Date: 10.11.22 - 14:11
%%% Ⓒ 2022 heyoka
%%%
%%% @doc
%%% version 2 of faxe's python interface, has a more streamlined data interface
%%% supporting complete data-point and data-batch python representations
%%% in python data items are still dicts

-module(c_python2).
-author("Alexander Minichmair").

-include("faxe.hrl").
%% API
-behavior(df_component).
%% API
-export([
   init/3, process/3,
   handle_info/2, options/0,
   call_options/2, get_python/0,
   shutdown/1]).

-callback execute(tuple(), term()) -> tuple().

-record(state, {
   node_id :: binary(),
   callback_module :: atom(),
   callback_class :: atom(),
   python_instance :: pid()|undefined,
   cb_object :: term(),
   func_calls = [],
   as :: binary()|undefined
}).


%% python method calls
-define(PYTHON_INFO_CALL, info).
-define(PYTHON_INIT_CALL, init).
-define(PYTHON_BATCH_CALL, batch).
-define(PYTHON_POINT_CALL, point).


-spec options() -> list(
   {atom(), df_types:option_name()} |
   {atom(), df_types:option_name(), df_types:option_value()}
).
options() -> [{cb_module, atom}, {cb_class, atom}, {as, string, undefined}].

%% @doc get the options to recognize in dfs for the python node (from the callback class)
-spec call_options(atom(), atom()) -> list(tuple()).
call_options(Module, Class) ->
   process_flag(trap_exit, true),
   P = get_python(),
   ModClass = list_to_atom(atom_to_list(Module)++"."++atom_to_list(Class)),
   Res =
      try pythra:func(P, ModClass, ?PYTHON_INFO_CALL, [Class]) of
         B when is_list(B) -> [{as, string, undefined}] ++ B
      catch
         _:{python,'builtins.ModuleNotFoundError', Reason,_}:_Stack ->
            Err = lists:flatten(io_lib:format("python module not found: ~s",[Reason])),
            {error, Err}
      end,
   python:stop(P),
   Res.

init(NodeId, _Ins, #{cb_module := Callback, cb_class := CBClass, as := As} = Args) ->
   PInstance = get_python(),
   %% create an instance of the callback class
   PyOpts = maps:without([cb_module, cb_class], Args#{<<"erl">> => self()}),
   ClassInstance = pythra:init(PInstance, Callback, CBClass, [PyOpts]),
   State = #state{
      callback_module = Callback,
      callback_class =  CBClass,
      cb_object = ClassInstance,
      node_id = NodeId,
      python_instance = PInstance,
      as = As},
   {ok, all, State}.

process(_Inp, #data_batch{} = Batch,
    State = #state{callback_module = _Mod, python_instance = Python, cb_object = Obj}) ->
   Data = to_map(Batch),
   NewObj = pythra:method(Python, Obj, ?PYTHON_BATCH_CALL, [Data]),
   {ok, State#state{cb_object = NewObj}}
;
process(_Inp, #data_point{} = Point, State = #state{python_instance = Python, cb_object = Obj}) ->

   NewObj = pythra:method(Python, Obj, ?PYTHON_POINT_CALL, [to_map(Point)]),
   {ok, State#state{cb_object = NewObj}}.

%% python sends us data
handle_info({emit_data, #{<<"fields">> := _F}= Data} , State = #state{as = As}) ->
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
handle_info({emit_data, Data0}, State = #state{as = As}) when is_map(Data0) ->
   Point = flowdata:point_from_json_map(Data0),
   {emit, {1, from_map(Point, As)}, State};
handle_info({emit_data, Data}, State = #state{as = As}) when is_list(Data) ->
   Points = [flowdata:point_from_json_map(D) || D <- Data],
   NewPoints = [from_map(Point, As) || Point <- Points],
   Batch = #data_batch{points = NewPoints},
   {emit, {1, Batch}, State};
handle_info({emit_data, {"Map", Data}}, State) when is_list(Data) ->
%%   lager:notice("got point data from python: ~p", [Data]),
   {emit, {1, Data}, State};
handle_info({python_error, Error}, State) ->
   lager:error("~p", [Error]),
   {ok, State};
handle_info({python_log, Message, Level0} = L, State) ->
   Level = log_level(faxe_util:to_bin(Level0)),
   esp_debug:do_log(Level, "~p", [Message]),
   {ok, State};
%%%
handle_info(start_debug, State) ->
   {ok, State};
handle_info(Request, State) ->
   lager:notice("got unexpected: ~p", [Request]),
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


get_python() ->
   {ok, PythonParams} = application:get_env(faxe, python),
   Path = proplists:get_value(script_path, PythonParams, "./python"),
   FaxePath = filename:join(code:priv_dir(faxe), "python/"),
   {ok, Python} = pythra:start_link([FaxePath, Path]),
   Python.


log_level(<<"debug">>) -> debug;
log_level(<<"info">>) -> info;
log_level(<<"notice">>) -> notice;
log_level(<<"warning">>) -> warning;
log_level(<<"error">>) -> error;
log_level(<<"alert">>) -> alert;
log_level(_) -> notice.
