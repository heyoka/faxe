import importlib
import inspect
import json
import os
# import modulefinder
import faxe_modulefinder
import sys

from erlport.erlterms import Atom
from erlport.erlang import set_message_handler

callback_object = None


def register_handler(_classname):

    def handler(args=None):
        # print("handler got", args)
        tag = args[0]
        args = args[1:]
        global callback_object
        if tag == b'init':
            class_name = args[0].decode('utf-8')
            module_name = class_name.lower()
            module = importlib.import_module(module_name)
            class_ = getattr(module, class_name)
            callback_object = class_(args[1])
            if '_state' in args[1]:
                args[1]['_state'] = json.loads(args[1]['_state'], object_hook=undefined_to_None)
                # print('persistent state for ', module_name, args[1]['_state'])

            # modulepath = os.path.abspath(module.__file__)
            # spath = args[1]['_ppath']
            # print('modulefinder on path', modulepath)
            # print('search in path', spath),
            # idict = get_imported_modules(modulepath, spath)
            # idict = get_imports2(module_name, spath, dict())
            # print("***", idict, '***')
        elif tag == b'point':
            callback_object.point(args[0])
        elif tag == b'batch':
            data = json.loads(args[0], object_hook=undefined_to_None)
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


def get_imports(module_name, spath, out):
    module = importlib.import_module(module_name)
    newdict = get_imported_filepaths(module, spath)
    out.update(newdict)
    if len(newdict) == 0:
        return out
    for modulename in newdict:
        inner = get_imports(modulename, spath, out)
        out.update(inner)
    return out


def get_imports2(module, spath, out):
    while True:
        if module in out:
            return out
        module_obj = importlib.import_module(module)
        newdict = get_imported_filepaths(module_obj, spath)
        refdict = newdict.copy()
        for refname in newdict:
            if refname in out:
                del refdict[refname]
        if len(refdict) == 0:
            break
        out.update(refdict)
        for modulename in refdict:
            inner = get_imports(modulename, spath, out)
            if len(inner) == 0:
                break
            out.update(inner)

    return out


def get_imported_filepaths(module, spath):
    modules = inspect.getmembers(module)
    results = list(filter(lambda m: inspect.ismodule(m[1]), modules))
    imported = dict()
    for k, mod in results:
        if hasattr(mod, '__file__'):
            filepath = mod.__file__
            if filepath.startswith(spath):
                imported[k] = filepath

    return imported


### deprecated/ not used

def get_imported_modules(modulepath, spath):
    out = dict()
    # finder = modulefinder.ModuleFinder(path=spath)
    finder = faxe_modulefinder.ModuleFinder(spath)
    finder.run_script(modulepath)
    for name, mod in finder.modules.items():
        filepath = str(mod.__file__)
        print("module filepath", filepath)
        if filepath.startswith(spath):
            print("module ", name, filepath)
            out[name] = filepath

    return out

