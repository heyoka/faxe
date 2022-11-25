import json

from erlport.erlterms import Atom
from erlport.erlang import set_message_handler

callback_object = None


def register_handler(_classname):

    def handler(args=None):
        tag = args[0]
        args = args[1:]
        global callback_object
        if tag == b'init':
            class_name = args[0].decode('utf-8')
            module_name = class_name.lower()
            module = __import__(module_name)
            class_ = getattr(module, class_name)
            callback_object = class_(args[1])
        elif tag == b'point':
            callback_object.point(args[0])
        elif tag == b'batch':
            data = json.loads(args[0], object_hook=undefined_to_None)
            print("json", data)
            callback_object.batch(data)
        else:
            print('no route for handler with', tag)

    set_message_handler(handler)
    return Atom(b'ok')


def undefined_to_None(dct):
    for k, v in dct.items():
        if v == 'undefined':
            dct[k] = None

    return dct

