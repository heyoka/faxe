import erlport.erlterms


# Mirrors all points it receives back to faxe
class Mirror:

    def __init__(self, args):
        self.args = args
        print("init mirror with ",  args, type(args))
        print("cast to dict: ", dict(args))
        print("foo is ", args[b'foo'])
        print("this is my info() ", Mirror.info())
        # for x in dict(args).keys:
        #     print("key " ,x)
        cb_args = {}
        for k, v in args.items():
            cb_args[str(k)] = v
        print("callback_args are:", cb_args)

    @staticmethod
    def info():
        outList = []
        for i, x in enumerate(Mirror.options()):
            l = list(x)
            l[0] = erlport.erlterms.Atom(l[0])
            l[1] = erlport.erlterms.Atom(l[1])
            outList.append(tuple(l))
        return outList

    # this is to implemented by a subclass
    def options():
        opts = [
            (b"foo", b"string"),
            (b"bar", b"float", 22.5),
            (b"baz", b"integer")
        ]
        return opts

    def init(self, init_req=None):
        print("hey you called init with : ", init_req)
        ret = {"eins": erlport.erlterms.List([1, 2, 3, 4]), "zwei": 2, "drei": {"view": 3}}
        return erlport.erlterms.Map(ret)

    def batch(self, req):
        print("batch at python: ", req)
        return req

    def point(self, req):
        print("point at python: ", req)
        return req
