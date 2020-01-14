from collections.abc import MutableMapping
import erlport


class DecodeDict(MutableMapping):
    """ The Faxe Framework produces dicts with bytes as keys instead of str.
    This dict is used to wrap these dicts and to allow using normal strings to get items """

    def __init__(self, *args, **kwargs):
        self.store = dict()
        self.own_key_type = str
        self.update(dict(*args, **kwargs))  # use the free update to set keys
        if len(self.store) > 0:
            self.own_key_type = type(next(iter(self.store)))

    def __getitem__(self, key):
        val = self.store[self.__keytransform__(key)]
        if isinstance(val, dict):
            return DecodeDict(val) # if it is a dict then also wrap the sub-dict
        else:
            return val

    def __setitem__(self, key, value):
        self.store[self.__keytransform__(key)] = value

    def __delitem__(self, key):
        del self.store[self.__keytransform__(key)]

    def __iter__(self):
        return iter(self.store)

    def __len__(self):
        return len(self.store)

    def __contains__(self, key):
        return self.store.__contains__(self.__keytransform__(key))

    def __keytransform__(self, key):
        if self.own_key_type == erlport.erlterms.Atom:
            return key.encode("utf-8")
        elif self.own_key_type == bytes:
            if isinstance(key, str):
                return key.encode("utf-8")
            elif isinstance(key, bytes):
                return key
            else:
                raise RuntimeError(f"could not decode key of type: {type(key)} ... expected key types is {self.own_key_type}")
        else:
            return key


if __name__ == "__main__":
    testDict = {b"hello" : 123, b"sub": {b"nested": "dict", b"also_nested": "str"}}
    decoded = DecodeDict(testDict)
    print(decoded["hello"])
    print(decoded["sub"]["nested"])

    if "hello" in decoded:
        print("in keyword works!")
    else:
        print("in keyword does not work!!!!")

    testDict2 = {"hello" : 123, "sub": {"nested": "dict", "also_nested": "str"}}
    decoded = DecodeDict(testDict2)
    print(decoded["hello"])
    print(decoded["sub"]["nested"])