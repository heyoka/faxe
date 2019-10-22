%% Date: 05.01.17 - 14:11
%% Ⓒ 2017 heyoka
-module(c_python).
-author("Alexander Minichmair").

-include("faxe.hrl").
%% API
-behavior(df_component).
%% API
-export([init/3, process/3, handle_info/2, options/0, call_options/2, get_python/0, shutdown/1]).

-callback execute(tuple(), term()) -> tuple().

-record(state, {
   node_id :: binary(),
   callback_module :: atom(),
   callback_class :: atom(),
   python_instance :: pid()|undefined,
   cb_object :: term(),
   func_calls = []
}).


%% defaults
-define(PYTHON_VERSION, "3").
-define(PYTHON_PATH, "/home/heyoka/workspace/faxe/python/").

%% python method calls
-define(PYTHON_INFO_CALL, "info").
-define(PYTHON_INIT_CALL, "init").
-define(PYTHON_BATCH_CALL, "batch").
-define(PYTHON_POINT_CALL, "point").



-spec options() -> list(
   {atom(), df_types:option_name()} |
   {atom(), df_types:option_name(), df_types:option_value()}
).
options() -> [{cb_module, atom}, {cb_class, atom}].

%% @doc get the options required for the python callback class
-spec call_options(atom(), atom()) -> list(tuple()).
call_options(Module, Class) ->
   P = get_python(),
   {Call, _} = build_class_call(Class, ?PYTHON_INFO_CALL, #state{}),
   Res = python:call(P, Module, Call, []),
   lager:warning("python info call to ~p gives ~p",[{Module, Class}, Res]),
   python:stop(P),
   Res.

init(NodeId, _Ins, #{cb_module := Callback, cb_class := CBClass} = Args) ->
   ArgsKeys = maps:keys(Args),
   lager:info("ArgsKeys: ~p",[ArgsKeys]),
   lager:notice("ARgs for ~p: ~p", [Callback, Args]),
   PInstance = get_python(),
   %% create an instance of the callback class
   {'$erlport.opaque', python, ClassInstance} =
      python:call(PInstance, Callback, CBClass, [maps:without([cb_module, cb_class], Args)]),
   %% call "init" on that instance
%%   _InitRes = python:call(PInstance, Callback, build_class_call(CBClass, ?PYTHON_INIT_CALL), [Args]),
%%   lager:notice("python init gives us: ~p",[_InitRes]),
   State = #state{
      callback_module = Callback,
      callback_class =  CBClass,
      cb_object = ClassInstance,
      node_id = NodeId,
      python_instance = PInstance},
   {ok, all, State}.

process(_Inport, #data_batch{} = Batch, State = #state{callback_module = Mod, python_instance = Python,
   cb_object = Obj, callback_class = Class}) ->
   lager:notice("from batch to list of maps: ~p",[flowdata:to_map(Batch)]),
   Data = flowdata:to_map(Batch),
%%   Res = python:call(Python, Mod, build_class_call(Class, ?PYTHON_BATCH_CALL), [Obj, Data]),
   {Call, NewState} = build_class_call(Class, ?PYTHON_BATCH_CALL, State),
   {T, Res} =
      timer:tc(python, call, [Python, Mod, Call, [Obj, Data]]),
   lager:notice("~p emitting: ~p after: ~p",[Mod, Res, T]),
   {emit, Res, NewState}
;
process(_Inport, #data_point{} = Point, State = #state{python_instance = Python, callback_module = Mod,
   cb_object = Obj, callback_class = Class}) ->

   Data = to_data_mapstruct(Point),
   lager:notice("DataPoint data: ~p",[Data]),
   {Call, NewState} = build_class_call(Class, ?PYTHON_POINT_CALL, State),
   {T, Res} =
      timer:tc(python, call, [Python, Mod, Call, [Obj, Data]]),
   lager:info("~p emitting: ~p after: ~p",[Mod, Res, T]),
   {emit, Res, NewState}.

handle_info(Request, State) ->
   io:format("~p request: ~p~n", [State, Request]),
   {ok, State}.

shutdown(#state{python_instance = Python}) ->
   python:stop(Python).

%%%%%%%%%%%%%%%%%%%% internal %%%%%%%%%%%%

get_python() ->
   {ok, PythonParams} = application:get_env(faxe, python),
   Version = proplists:get_value(version, PythonParams, ?PYTHON_VERSION),
   Path = proplists:get_value(script_path, PythonParams, ?PYTHON_PATH),
   {ok, Python} = python:start_link([{python_path, Path},{python, "python"++Version}]),
   Python.

build_class_call(Class, Func, State = #state{func_calls = Calls}) when is_atom(Class), is_list(Func) ->
   case proplists:get_value(Func, Calls) of
      undefined -> SMod = atom_to_list(Class) ++ "." ++ Func,
                  Call = list_to_atom(SMod),
         {Call, State#state{func_calls = [{Func,Call} | Calls]}};
      FuncCall -> {FuncCall, State}
   end.



-define(DEFAULT_ID, <<"00000">>).
-define(DEFAULT_VS, 1).
-define(DEFAULT_DF, <<"00.000">>).

to_data_mapstruct(P=#data_point{ts = Ts, fields = Fields, tags = _Tags}) ->
   DataFields =
      case maps:get(<<"data">>, Fields, nil) of
         nil -> maps:without([<<"id">>,<<"vs">>,<<"df">>], Fields);
         Data -> Data
      end,
%%   ML = maps:to_list(DataFields),
%%   Cleaned = maps:from_list([{binary_to_list(Key), Val} || {Key, Val} <- ML]),
%%   lager:info("Datafields for JSON: ~p", [DataFields]),
   #{<<"ts">> => Ts,
      <<"id">> => flowdata:field(P ,<<"id">>, ?DEFAULT_ID),
      <<"vs">> => flowdata:field(P, <<"vs">>, ?DEFAULT_VS),
      <<"df">> => flowdata:field(P, <<"df">>, ?DEFAULT_DF),
      <<"data">> => DataFields}.