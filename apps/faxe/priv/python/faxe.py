import importlib
import json
import os
import sys
import time
from functools import cmp_to_key
from jsonpath_ng import parse
import pickle

from erlport.erlterms import Atom, Map, List
from erlport.erlang import cast

import inspect
import faxe_modulefinder


class Faxe:
    """
    base class for faxe's custom user nodes, written in python
    """

    LOG_LEVEL_DEBUG = 'debug'
    LOG_LEVEL_INFO = 'info'
    LOG_LEVEL_NOTICE = 'notice'
    LOG_LEVEL_WARNING = 'warning'
    LOG_LEVEL_ERROR = 'error'
    LOG_LEVEL_CRITICAL = 'critical'
    LOG_LEVEL_ALERT = 'alert'

    STATE_MODE_HANDLE   = 'handle'
    STATE_MODE_EMIT     = 'emit'
    STATE_MODE_MANUAL   = 'manual'

    ERL_CAST_PERSIST_STATE = Atom(b'persist_state')
    ERL_CAST_EMIT = Atom(b'emit_data')
    ERL_CAST_ERROR = Atom(b'python_error')
    ERL_CAST_LOG = Atom(b'python_log')

    ARGS_ERL_PID = '_erl'
    ARGS_STATE = '_state'
    ARGS_PATH = '_ppath'

    CALLBACK_OPTIONS = 'options'

    def __init__(self, args):
        self._erlang_pid = args[Faxe.ARGS_ERL_PID]
        self._last_state = None
        self._pstate = None
        if Faxe.ARGS_STATE in args:
            self._pstate = pickle.loads(args[Faxe.ARGS_STATE])
            del args[Faxe.ARGS_STATE]
        decoded = Faxe.decode_args(args)
        # print("state", self.get_state())
        del decoded[Faxe.ARGS_PATH]
        del decoded[Faxe.ARGS_ERL_PID]
        # init subclass
        self.init(decoded)

    @staticmethod
    def info(clname):
        _modname, module = Faxe.import_module(clname)
        cls = getattr(module, clname)
        method = getattr(cls, Faxe.CALLBACK_OPTIONS)
        outlist = []
        for i, x in enumerate(method()):
            lout = list(x)
            if 3 == len(lout) and isinstance(lout[2], str):
                lout[2] = to_bytes(lout[2])
            lout[0] = Atom(to_bytes(lout[0]))
            lout[1] = Atom(to_bytes(lout[1]))
            outlist.append(tuple(lout))
        return outlist

    @staticmethod
    def fetch_deps(clname, spath):
        modname, module = Faxe.import_module(clname)
        module_path = os.path.abspath(module.__file__)
        deps_list = get_imported_modules(module_path, spath)
        return deps_list

    @staticmethod
    def import_module(clname):
        modname = clname.lower()
        module = importlib.import_module(modname)
        return modname, module

    @staticmethod
    def options():
        """
        optional
        overwrite this method to request options you would like to use

        return value is a list of tuples: (option_name, option_type, (optional: default type))
        a two tuple: ("foo", "string") with no default value is mandatory in the dfs script
        a three tuple: ("foo", "string", "mystring") may be overwritten in a dfs script

        :return: list of tuples
        """
        return list()

    def init(self, args=None):
        """
        optional
        will be called on startup with args requested with options()
        :param args: dict
        """
        pass

    def handle_point(self, _point_data: dict):
        """
        optional
        called when a data_point comes in to this node
        :param _point_data: dict @see class Point
        """
        self.log('handle_point not implemented!', 'error')

    def handle_batch(self, _batch_data: dict):
        """
        optional
        called when a data_batch comes in
        :param _batch_data: dict @see class Batch
        """
        self.log('handle_batch not implemented!', 'error')

    def emit(self, emit_data: dict):
        """
        used to emit data to downstream nodes
        :param emit_data: dict
        """
        data = None
        if 'points' in emit_data and isinstance(emit_data['points'], list):
            data = json.dumps(emit_data)
        elif 'fields' in emit_data and isinstance(emit_data['fields'], dict):
            data = encode_data(emit_data)

        if data is not None:
            cast(self._erlang_pid, (Faxe.ERL_CAST_EMIT, data))

        if self._state_opts() == Faxe.STATE_MODE_EMIT:
            self.persist_state()

    # persisted state handling #######################################################

    def _state_opts(self):
        return self.state_mode()

    def state_mode(self):
        """
        overwrite this method in subclasses to return one of the state modes
        :return:
        """
        # state_mode:
        #   ('auto'(every occasion/init+emit+handle) )
        #   'handle'(after every handle_point and/or handle_batch function)
        #   'emit'(after every call to self.emit)
        #   'manual'(call state_persist on you own)
        #   'none'(no persistent state handling)
        return Faxe.STATE_MODE_MANUAL

    def send_state(self, state_data):
        pickle_start = time.time_ns()
        state = pickle.dumps(state_data, protocol=pickle.HIGHEST_PROTOCOL)
        dur = int((time.time_ns() - pickle_start) / 1000)
        # print("state size", sys.getsizeof(state), "took my", dur)
        if state != self._last_state:
            cast(self._erlang_pid, (Faxe.ERL_CAST_PERSIST_STATE, state))

        self._last_state = state

    def format_state(self):
        myvars = vars(self)
        pdata = dict()
        for ename, evalue in myvars.items():
            if type(evalue) in (str, int, float, dict, list, tuple, set, complex, range, bool, bytes, bytearray):
                pdata[ename] = evalue
        return pdata

    def persist_state(self, state=None):
        pdata = state
        if pdata is None:
            pdata = self.format_state()
        # delete Faxe members
        if type(pdata) is dict:
            Faxe.dict_del(pdata, '_pstate')
            Faxe.dict_del(pdata, '_erlang_pid')
            Faxe.dict_del(pdata, '_last_state')
        # send to erl
        if pdata is not None: # why not ?
            # print("persist state", self._state_opts(), pdata)
            self.send_state(pdata)

    def get_state(self):
        """
        get the last persisted state data, that was given to this node
        :return: any
        """
        return self._pstate

    def get_state_value(self, key, default):
        """
        get a specific entry from the state, if state is a dict, otherwise returns 'default'
        :param key: string
        :param default: any
        :return: any
        """
        if type(self._pstate) == dict:
            if key in self._pstate:
                return self._pstate[key]
        return default

    # logging ###############################################################
    def error(self, error):
        """
        @deprecated use log(msg, level='error') instead
        used to send an error back to faxe
        :param error: string
        """
        cast(self._erlang_pid, (Faxe.ERL_CAST_ERROR, error))

    def log(self, msg, level=LOG_LEVEL_NOTICE):
        """
        used to send a log message
        :param msg: string
        :param level: 'debug' | 'info' | 'notice' | 'warning' | 'error' | 'critical' | 'alert'
        """
        cast(self._erlang_pid, (Faxe.ERL_CAST_LOG, msg, level))

    @staticmethod
    def now():
        """
        unix timestamp in milliseconds (utc)
        :return: int
        """
        return int(time.time()*1000)

    def point(self, req):
        self.handle_point(dict(req))
        if self._state_opts() == Faxe.STATE_MODE_HANDLE:
            self.persist_state()
        return self

    def batch(self, req):
        self.handle_batch(req)
        if self._state_opts() == Faxe.STATE_MODE_HANDLE:
            self.persist_state()
        return self

    @staticmethod
    def dict_del(data_dict, dict_key):
        if dict_key in data_dict:
            del data_dict[dict_key]

    @staticmethod
    def is_nonclass_type(val):
        return getattr(val, "__dict__", None) is not None

    @staticmethod
    def decode_args(args):
        for i, arg in args.items():
            if isinstance(arg, list):
                outl = list()
                for listel in arg:
                    if isinstance(listel, bytes):
                        outl.append(listel.decode('utf-8'))
                    else:
                        outl.append(listel)

                    args[i] = outl
            elif isinstance(arg, bytes):
                args[i] = arg.decode('utf-8')
            elif isinstance(arg, Map):
                args[i] = Faxe.decode_args(arg)

        return args


class Point:
    """
    Completely static helper class for data-point structures (dicts)

    point = dict()
    point['ts'] = int millisecond timestamp
    point['fields'] = dict()
    point['tags'] = dict()
    point['dtag'] = int

    """

    @staticmethod
    def new(ts=None):
        p = dict()
        p['fields'] = dict()
        p['tags'] = dict()
        p['dtag'] = None
        p['ts'] = ts
        return p

    @staticmethod
    def fields(point_data, newfields=None):
        """
        get or set all the fields (dict)
        :param point_data: dict()
        :param newfields: dict()
        :return: dict()
        """
        if newfields is not None:
            point_data['fields'] = newfields
            return point_data

        return dict(point_data['fields'])

    @staticmethod
    def value(point_data, path, value=None):
        """
        get or set a specific field
        :param point_data: dict()
        :param path: string
        :param value: any
        :return: None, if field is not found /
        """
        if value is not None:
            Jsn.set(point_data, path, value)
            return point_data

        return Jsn.get(point_data, path)

    @staticmethod
    def values(point_data, paths, value=None):
        """
        get or set a list of fields
        :param point_data: dict
        :param paths: list
        :param value: any
        :return: point_data|list
        """
        if value is not None:
            for path in paths:
                Jsn.set(point_data, path, value)
            return point_data

        out = list()
        for path in paths:
            out.append(Jsn.get(point_data, path))
        return out

    @staticmethod
    def default(point_data, path, value):
        """

        :param point_data:
        :param path:
        :param value:
        :return:
        """
        if Point.value(point_data, path) is None:
            Point.value(point_data, path, value)

        return point_data

    @staticmethod
    def tags(point_data, newtags=None):
        if newtags is not None:
            point_data['tags'] = newtags
            return point_data

        return point_data['tags']

    @staticmethod
    def ts(point_data, newts=None):
        """
        get or set the timestamp of this point
        :param point_data: dict
        :param newts: integer
        :return: integer|dict
        """
        if newts is not None:
            point_data['ts'] = int(newts)
            return point_data

        return point_data['ts']

    @staticmethod
    def dtag(point_data, newdtag=None):
        if newdtag is not None:
            point_data['dtag'] = newdtag
            return point_data

        return point_data['dtag']


class Batch:
    """
    Completely static helper class for data-batch structures (dicts)

    batch = dict()
    batch['points'] = list() of point dicts sorted by their timestamps
    batch['start_ts'] = int millisecond unix timestamp denoting the start of this batch

    batch['dtag'] = int
    """

    @staticmethod
    def new(start_ts=None):
        b = dict()
        b['points'] = list()
        b['dtag'] = None
        b['start_ts'] = start_ts
        return b

    @staticmethod
    def empty(batch_data):
        """
        a Batch is empty, if it has no points
        :param batch_data:
        :return: True | False
        """
        return ('points' not in batch_data) or (batch_data['points'] == [])

    @staticmethod
    def points(batch_data, points=None):
        """

        :param points: None | list()
        :param batch_data: dict
        :return: list
        """
        if points is not None:
            batch_data['points'] = points
            Batch.sort_points(batch_data)
            return batch_data

        return list(batch_data['points'])

    @staticmethod
    def value(batch_data, path, value=None):
        """
        get or set path from/to every point in a batch
        :param batch_data:
        :param path:
        :param value:
        :return: list
        """
        out = list()
        points = batch_data['points']
        for p in points:
            out.append(Point.value(p, path, value))
        if value is not None:
            return batch_data

        return out

    @staticmethod
    def values(batch_data, paths, value=None):
        """
        get or set path from/to every point in a batch
        :param batch_data:
        :param paths:
        :param value:
        :return: list
        """
        if not isinstance(paths, list):
            raise TypeError('Batch.values() - paths must be a list of strings')
        if value is not None:
            points = batch_data['points']
            for p in points:
                for path in paths:
                    Point.value(p, path, value)
            # batch_data['points'] = points
            return batch_data
        else:
            out = list()
            points = batch_data['points']
            for p in points:
                odict = dict()
                for path in paths:
                    odict[path] = Point.value(p, path, value)
                out.append(odict)
            return out

    @staticmethod
    def default(batch_data, path, value):
        """

        :param batch_data:
        :param path:
        :param value:
        :return:
        """
        points = batch_data['points']
        for p in points:
            Point.default(p, path, value)
        return batch_data

    @staticmethod
    def dtag(batch_data):
        return batch_data['dtag']

    @staticmethod
    def start_ts(batch_data, newts=None):
        if newts is not None:
            batch_data['start_ts'] = newts
            return batch_data

        return batch_data['start_ts']

    @staticmethod
    def add(batch_data, point):
        if ('points' not in batch_data) or (type(batch_data['points']) != list):
            batch_data['points'] = list()
            batch_data['points'].append(point)
        else:
            batch_data['points'].append(point)
            Batch.sort_points(batch_data)

        return batch_data

    @staticmethod
    def sort_points(batch_data):
        points = batch_data['points']
        points = sorted(points, key=cmp_to_key(Batch.sort_ts))
        batch_data['points'] = points

    @staticmethod
    def sort_ts(a, b):
        """
        sorts data-point dicts by their timestamps ascending
        :param a: dict()
        :param b: dict()
        :return: int
        """
        if a['ts'] < b['ts']:
            return -1
        elif a['ts'] == b['ts']:
            return 0

        return 1


class Jsn:

    @staticmethod
    def get(point_data, path):
        jexpr = parse('$.' + path)
        match = jexpr.find(point_data['fields'])
        try:
            return match[0].value
        except IndexError:
            return None

    @staticmethod
    def set(point_data, path, data):
        jexpr = parse('$.' + path)
        jexpr.update_or_create(point_data['fields'], data)


"""
    ### encoder functions
    convert every string in data structure to bytes, 
    so they become erlang binaries
"""


def encode_data(data):
    if isinstance(data, Map):
        return encode_data(dict(data))
    if isinstance(data, List):
        return encode_data(list(data))
    if type(data) == list:
        newlist = [encode_data(d) for d in data]
        return newlist
    if type(data) == dict:
        return encode_dict(data)

    return data


def encode_dict(mydict):
    return {to_bytes(k): to_bytes(v) for (k, v) in mydict.items()}


def to_bytes(ele):
    if type(ele) == str:
        return ele.encode("utf-8")
    if type(ele) == dict:
        return encode_dict(ele)
    if type(ele) == list:
        return [to_bytes(d) for d in ele]
    if isinstance(ele, Map):
        return encode_dict(dict(ele))
    if isinstance(ele, List):
        return [to_bytes(d) for d in ele]

    return ele


def get_imported_modules(module_path, spath):
    out_list = list()
    finder = faxe_modulefinder.ModuleFinder(path=[os.path.dirname(spath)])
    finder.run_script(module_path)
    for name, mod in finder.modules.items():
        filepath = str(mod.__file__)
        if filepath.startswith(spath) and filepath != module_path and name != '__main__':
            out_list.append(name.encode("utf-8"))
    return out_list
