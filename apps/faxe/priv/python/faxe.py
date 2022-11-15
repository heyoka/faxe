import erlport.erlterms
import erlport.erlang
from decode_dict import DecodeDict


"""
    we always want to have strings as bytes for faxe
"""


def encode_data(data):
    if isinstance(data, DecodeDict):
        return data.store
    if isinstance(data, erlterms.Map):
        encode_data(dict(data))
    if type(data) == list:
        newlist = [encode_data(d) for d in data]
        return newlist
    if type(data) == dict:
        return encode_dict(data)


def encode_dict(mydict):
    return {to_bytes(k): to_bytes(v) for (k, v) in mydict.items()}


def to_bytes(ele):
    if type(ele) == str:
        return ele.encode("utf-8")
    elif type(ele) == dict:
        return encode_dict(ele)

    return ele


class Faxe:
    """
    base class for faxe's custom user nodes written in python
    """

    def __init__(self, args):
        self.erlang_pid = args[b'erl']
        self.init(DecodeDict(dict(args)))

    @staticmethod
    def info(clname):
        classname = clname
        modname = classname.lower()
        module = __import__(modname)
        cls = getattr(module, classname)
        method = getattr(cls, "options")
        outlist = []
        for i, x in enumerate(method()):
            l = list(x)
            l[0] = erlport.erlterms.Atom(to_bytes(l[0]))
            l[1] = erlport.erlterms.Atom(to_bytes(l[1]))
            outlist.append(tuple(l))
        return outlist

    @staticmethod
    def options():
        """
        overwrite this method to request options you would like to use

        return value is a list of tuples: (option_name, option_type, (optional: default type))
        a two tuple: ("foo", "string") with no default value is mandatory in the dfs script
        a three tuple: ("foo", "string", "mystring") may be overwritten in a dfs script

        :return: list of tuples
        """
        return list()

    def init(self, args=None):
        """
        will be called on startup with args requested with options()
        :param args: dict
        """
        pass

    def handle_point(self, point_data):
        """
        called when a data_point comes in to this node
        :param point_data: dict
        """
        pass

    def handle_batch(self, batch_data):
        """
        called when a data_batch comes in
        :param batch_data: list of dicts
        """
        pass

    def batch(self, req):
        batch = []
        for point in req:
            batch.append(DecodeDict(point))
        self.handle_batch(batch)
        return self

    def point(self, req):
        self.handle_point(DecodeDict(req))
        return self

    def emit(self, emit_data):
        """
        used to emit data to the next node(s)
        :param emit_data: dict | DecodeDict | list of dicts | list of DecodeDict
        """
        if type(emit_data) == dict or isinstance(emit_data, DecodeDict) or type(emit_data) == list:
            erlport.erlang.cast(self.erlang_pid,
                                (erlport.erlterms.Atom(b'emit_data'), encode_data(emit_data)))

    def error(self, error):
        """
        used to send an error back
        :param error: string
        """
        erlport.erlang.cast(self.erlang_pid, (erlport.erlterms.Atom(b'python_error'), error))

    def log(self, msg):
        """
        used to send a log
        :param msg: string
        """
        erlport.erlang.cast(self.erlang_pid, (erlport.erlterms.Atom(b'python_log'), msg))
