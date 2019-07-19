import erlport.erlterms


# Mirrors all points it receives back to faxe
class Mirror:

    @staticmethod
    def info():
        print("info called")
        li = erlport.erlterms.List([(erlport.erlterms.Atom(b"foo"), erlport.erlterms.Atom(b"string"))])
        return li

    def init(self, init_req=None):
        print("hey you called init ?!")
        ret = {"eins": erlport.erlterms.List([1, 2, 3, 4]), "zwei": 2, "drei": {"view": 3}}
        return erlport.erlterms.Map(ret)

    def batch(self, req):
        print("batch at python: ", req)
        return req

    def point(self, req):
        print("point at python: ", req)
        return req
