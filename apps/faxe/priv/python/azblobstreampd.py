from azure.storage.blob import BlobServiceClient
from azure.storage.blob import ContainerClient
import erlport
import erlport.erlang
import erlport.erlterms
import faxe
from decode_dict import DecodeDict
import json
import sys
from io import BytesIO
import pandas as pd
import numpy as np

# this is a pointer to the module object instance itself.
this = sys.modules[__name__]

format_csv: bytes = b'csv'
format_json: bytes = b'json'

meta_dict: bytes = b'meta'
data_dict: bytes = b'data'
line_field: bytes = b'line'
header_field: bytes = b'header'
eof_field: bytes = b'done'
chunk_field: bytes = b'chunk'

empty: str = ''


def bytes2string(byte_string):
    if type(byte_string) == bytes:
        return byte_string.decode("utf-8")
    else:
        return byte_string


def prepare(args):

    this.blob_client = None
    this.args = DecodeDict(dict(args))
    this.erlang_pid = this.args['erl']
    this.line_format = this.args['format']
    this.container = bytes2string(this.args['container'])
    this.blob_name = bytes2string(this.args['blob_name'])
    this.encoding = bytes2string(this.args['encoding'])
    this.header_row = int(this.args['header_row'])
    this.data_row = int(this.args['data_start_row'])
    this.line_separator = this.args['line_separator']
    this.column_separator = bytes2string(this.args['column_separator'])
    this.batch_size = int(this.args['batch_size'])

    # this.encoding = 'utf-8'

    this.current_chunk = 0
    this.current = empty
    this.current_batch = []
    this.current_row = 0
    this.header = dict()

    # this.line_format = b'joeh'
    if this.line_format not in [format_csv, format_json]:
        error('Unknown format ' + bytes2string(this.line_format))
        return False

    # this.args['account_url'] = 'somebullshit'
    # this.args['az_sec'] = 'haaa'
    try:
        blob_service_client = BlobServiceClient(bytes2string(this.args['account_url']),
                                                bytes2string(this.args['az_sec']),
                                                max_chunk_get_size=this.args['chunk_size'],
                                                max_single_get_size=this.args['chunk_size'])
    except Exception as Err:
        error('While trying to instanciate BlobServiceClient an Error occured: ' + str(Err) + " account_url=" +
              bytes2string(this.args['account_url']))
        return False


    prepare_meta()
    # this.container = 'somebullshit'
    container_client = blob_service_client.get_container_client(this.container)
    if not isinstance(container_client, ContainerClient):
        return False

    # try:
    #     container_client = blob_service_client.get_container_client(this.container)
    # except azure.core.exceptions.ResourceNotFoundError as RC:
    #     error('While trying to instanciate ContainerClient a ResourceNotFoundError occured: ' + str(RC))
    #     return False
    # except Exception as ErrCClient:
    #     error('While trying to instanciate ContainerClient an Error occured: ' + str(ErrCClient))
    #     return False
    # print("SETUP2")
    # this.blob_name = 'wuzuwzue'
    try:
        this.blob_client = container_client.get_blob_client(this.blob_name)
        erlport.erlang.set_message_handler(start)
        return True
    # except azure.core.exceptions.ResourceNotFoundError as RE:
    #     error('While trying to instanciate BlobClient a ResourceNotFoundError occured: ' + str(RE))
    #     return False
    except Exception as E:
        error('While trying to instanciate BlobClient an Error occured: ' + str(E))
        return False


def start(_arg):
    if this.line_format == format_csv:
        handle_csv()
    else:
        # json
        stream = this.blob_client.download_blob()
        for chunk in stream.chunks():
            this.current += chunk.decode(this.encoding)
            this.current_chunk += 1
            extract_rows()

        if this.current.endswith('\n'):
            this.current = this.current[:-1]
        if this.current != empty:
            this.current_row += 1
            exportjson(this.current)

        if len(this.current_batch) > 0:
            emit(this.current_batch)

    # we are done
    emit({eof_field: True})


def extract_rows():
    addnl = ''
    if this.current.endswith(('\n' '\r', '\x1c', '\x85', '\x0b', u"\u2028", u"\u2029")):
        print("**************** endswith newline")
        addnl = '\n'
    splitted = this.current.splitlines()

    last = splitted.pop()
    for line in splitted:
        this.current_row += 1
        exportjson(line)
        # print(line)

    print("last", last+addnl)
    this.current = last+addnl


def exportjson(row):
    converted = handle_json(row)
    if converted is not None:
        this.current_batch.append(converted)
        if len(this.current_batch) >= this.batch_size:
            # print("batch", this.current_batch)
            emit(this.current_batch)
            this.current_batch = []


def handle_csv():
    print("start file download...")
    with BytesIO() as input_blob:
        this.blob_client.download_blob().readinto(input_blob)
        input_blob.seek(0)
        # this.batch_size = this.batch_size
        for chunk in pd.read_csv(
                input_blob,
                header=this.header_row-1,
                encoding=this.encoding,
                sep=this.column_separator,
                chunksize=this.batch_size,
                keep_default_na=False):
            this.current_chunk += 1
            retlist = chunk.to_dict(orient='records')
            outlist = []
            for entry in retlist:
                this.current_row += 1
                outlist.append({meta_dict: get_meta(), data_dict: entry})

            emit(outlist)
            # print(chunk.to_dict(orient='records'))


def handle_json(row):
    therow = json.loads(row)
    therow[meta_dict] = get_meta()
    print(therow)
    return therow


def get_meta():
    meta = this.args.copy()
    meta[line_field] = this.current_row
    meta[chunk_field] = this.current_chunk
    return meta


def prepare_meta():
    del this.args['account_url']
    del this.args['erl']
    del this.args['az_sec']
    this.args = dict(this.args)


def emit(emit_data):
    """
    used to emit data to the next node(s)
    :param emit_data: dict | DecodeDict | list of dicts | list of DecodeDict
    """
    if type(emit_data) == dict or isinstance(emit_data, DecodeDict) or type(emit_data) == list:
        erlport.erlang.cast(this.erlang_pid,
                            (erlport.erlterms.Atom(b'emit_data'), faxe.encode_data(emit_data)))


def error(error):
    """
    used to send an error back
    :param error: string
    """
    print("Error", error)
    erlport.erlang.cast(this.erlang_pid, (erlport.erlterms.Atom(b'python_error'), str(error)))

