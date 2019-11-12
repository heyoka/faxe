import erlport.erlterms
import erlport.erlang


class Faxe:

    __class = None

    def __init__(self, args):
        self.erlang_pid = args[b'erl']
        self.init(dict(args))

    @staticmethod
    def info(clname):
        classname = clname
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
        return self

    def point(self, req):
        self.handle_point(req)
        return self

    def emit(self, emit_data):
        erlport.erlang.cast(self.erlang_pid, (erlport.erlterms.Atom(b'emit_data'), emit_data))

    def error(self, error):
        erlport.erlang.cast(self.erlang_pid, (erlport.erlterms.Atom(b'python_error'), error))
