import erlport.erlterms
import erlport.erlang


class Faxe:
    """
    base class for faxe's custom user nodes written in python
    """

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
        """
        overwrite this method to request options you would like to use

        return value is a list of tuples: (option_name, option_type, (optional: default type))
        a two tuple: (b"foo", b"string") with no default value is mandatory in the dfs script
        a three tuple: (b"foo", b"string", b"mystring") may be overwritten in a dfs script

        :return: list of tuples
        """
        return list()

    def init(self, args=None):
        """
        will be called on startup with args requests with options()
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
        self.handle_batch(req)
        return self

    def point(self, req):
        self.handle_point(req)
        return self

    def emit(self, emit_data):
        """
        used to emit data to the next node(s)
        :param emit_data: dict | list of dicts
        """
        erlport.erlang.cast(self.erlang_pid, (erlport.erlterms.Atom(b'emit_data'), emit_data))

    def error(self, error):
        """
        used to send an error back
        :param error: string
        """
        erlport.erlang.cast(self.erlang_pid, (erlport.erlterms.Atom(b'python_error'), error))
