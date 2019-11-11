import erlport.erlterms
import erlport.erlang
import pandas as pd
import numpy as np


class Faxe:

    __class = None

    def __init__(self, args):
        print("__init__ in Faxe")
        print("args in __init__", args)
        self.cb_args = dict(args)
        self.erlang_pid = self.cb_args[b'erl']
        print("erl process is here ", self.erlang_pid)
        self.init(self.cb_args)

    @staticmethod
    def info(clname):
        classname = clname #.decode("utf-8")
        modname = classname.lower()
        module = __import__(modname)
        print("classname ", classname, " modname ", modname)
        cls = getattr(module, classname)
        method = getattr(cls, "options")
        outList = []
        for i, x in enumerate(method()):
            l = list(x)
            l[0] = erlport.erlterms.Atom(l[0])
            l[1] = erlport.erlterms.Atom(l[1])
            outList.append(tuple(l))
        return outList

    @staticmethod
    def options():
        pass

    def init(self, args=None):
        pass

    def handle_point(self, point_data):
        pass

    def handle_batch(self, batch_data):
        pass

    def batch(self, req):
        self.handle_batch(req)

    def point(self, req):
        self.handle_point(dict(req))

    def return_emit(self, emit_data):
        return erlport.erlterms.Atom(b'emit_data'), emit_data

    def return_ok(self):
        return erlport.erlterms.Atom(b'ok')

    def return_error(self, error):
        return erlport.erlterms.Atom(b'error'), error

    def emit(self, emit_data):
        erlport.erlang.cast(self.erlang_pid, (erlport.erlterms.Atom(b'emit_data'), emit_data))

    def error(self, error):
        erlport.erlang.cast(self.erlang_pid, (erlport.erlterms.Atom(b'python_error'), error))
