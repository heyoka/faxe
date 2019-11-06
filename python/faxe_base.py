import erlport.erlterms
import pandas as pd


def get_class(name):
    classname = name.decode("utf-8")
    modname = classname.lower()
    module = __import__(modname)
    print("classname ", classname, " modname ", modname)
    cls = getattr(module, classname)
    return classname, cls


class Faxe:

    def __init__(self, args):
        print("__init__ in Faxe")
        self.args = args
        self.__cbclassname, self.__cbclass = get_class(args[b'cb_class'])
        # print()
        # constructor = getattr(self.__cbclass, self.__cbclassname)
        cb_args = {}
        for k, v in args.items():
            cb_args[str(k)] = v

        self.callback = self.__cbclass(cb_args)

    def getcallable(self, callable_name):
        return getattr(self.__cbclass, callable_name)

    @staticmethod
    def info(clname):
        classname, class_attr = get_class(clname)
        method = getattr(class_attr, "options")
        outList = []
        for i, x in enumerate(method()):
            l = list(x)
            l[0] = erlport.erlterms.Atom(l[0])
            l[1] = erlport.erlterms.Atom(l[1])
            outList.append(tuple(l))
        return outList

    def batch(self, req):
        print("batch at python: ", self)
        df = pd.DataFrame.from_dict(req, orient='columns')
        df.set_index(keys=b'ts', inplace=True)
        # print(df)
        # mean = df.mean(axis=0)
        # print("the mean of vals is: ", mean),
        # req1 = req[b'mean']
        cb = self.getcallable("handle_batch")
        cb(df)

    def point(self, req):
        print("point at python: ", req)
        # req1 = dict(req)
        # df = pd.DataFrame(data=req)
        # print(df)
        # req1[b'vs'] = 5
        # req2 = {b"vs": 2, "id": "oi23u4oi23u4oi23u4oi2u34o2i3u4o", "df": "220.023",
        #         "data": {"val1": 23423.3}}
        cb = self.getcallable("handle_point")
        cb(req)
