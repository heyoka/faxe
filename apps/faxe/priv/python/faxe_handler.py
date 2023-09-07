import importlib
import inspect
import json
import os
# import modulefinder
import faxe_modulefinder
import sys
import psutil

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
            module = importlib.import_module(module_name)
            class_ = getattr(module, class_name)
            callback_object = class_(args[1])
        elif tag == b'point':
            callback_object.point(args[0])
        elif tag == b'batch':
            data = json.loads(args[0], object_hook=undefined_to_None)
            callback_object.batch(data)
        else:
            print('no route for handler with', tag)

    set_message_handler(handler)
    return Atom(b'ok')


def py_stats():
    return [(p.pid, p.info['name'], p.info['username'], p.info['memory_info'].rss) \
            for p in psutil.process_iter(['name', 'memory_info', 'username'])]


def process_stats():
    return {b'mem': process_mem_usage(), b'cpu_percent': process_cpu_usage()}


# MiB of ram residential
def process_mem_usage():
    mem = psutil.Process().memory_info()
    return round(mem.rss / (1024 * 1024), 2)


def process_cpu_usage():
    p = psutil.Process()
    return p.cpu_percent(interval=0)



def undefined_to_None(dct):
    for k, v in dct.items():
        if v == 'undefined':
            dct[k] = None

    return dct

