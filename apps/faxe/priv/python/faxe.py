import time
from functools import cmp_to_key
from jsonpath_ng import parse

import erlport.erlterms
import erlport.erlang


class Faxe:
    """
    base class for faxe's custom user nodes, written in python
    """

    def __init__(self, args):
        self.erlang_pid = args['erl']
        self.init(Faxe.decode_args(args))

    @staticmethod
    def info(clname):
        classname = clname
        modname = classname.lower()
        module = __import__(modname)
        cls = getattr(module, classname)
        method = getattr(cls, "options")
        outlist = []
        for i, x in enumerate(method()):
            lout = list(x)
            lout[0] = erlport.erlterms.Atom(to_bytes(lout[0]))
            lout[1] = erlport.erlterms.Atom(to_bytes(lout[1]))
            outlist.append(tuple(lout))
        return outlist

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
        erlport.erlang.cast(self.erlang_pid,
                            (erlport.erlterms.Atom(b'emit_data'), encode_data(emit_data)))

    def error(self, error):
        """
        @deprecated use log(msg, level='error') instead
        used to send an error back to faxe
        :param error: string
        """
        erlport.erlang.cast(self.erlang_pid, (erlport.erlterms.Atom(b'python_error'), error))

    def log(self, msg, level='notice'):
        """
        used to send a log message
        :param msg: string
        :param level: 'debug' | 'info' | 'notice' | 'warning' | 'error' | 'critical' | 'alert'
        """
        erlport.erlang.cast(self.erlang_pid, (erlport.erlterms.Atom(b'python_log'), msg, level))

    @staticmethod
    def now():
        """
        unix timestamp in milliseconds (utc)
        :return: int
        """
        return int(time.time()*1000)

    def point(self, req):
        self.handle_point(dict(req))
        return self

    def batch(self, req):
        self.handle_batch(dict(req))
        return self

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
        get or set a specific field
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
    if isinstance(data, erlport.erlterms.Map):
        return encode_data(dict(data))
    if isinstance(data, erlport.erlterms.List):
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
    if isinstance(ele, erlport.erlterms.Map):
        return encode_dict(dict(ele))
    if isinstance(ele, erlport.erlterms.List):
        return [to_bytes(d) for d in ele]

    return ele
