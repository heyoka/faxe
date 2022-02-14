from azure.storage.blob import BlobServiceClient
from azure.storage.blob import ContainerClient
import erlport
import erlport.erlang
import erlport.erlterms
import faxe
from decode_dict import DecodeDict
import json
import sys

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

empty: bytes = b''


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
    this.header_row = this.args['header_row']
    this.data_row = this.args['data_start_row']
    this.line_separator = this.args['line_separator']
    this.column_seperator = this.args['column_separator']
    this.batch_size = this.args['batch_size']

    this.current_chunk = 0
    this.current = empty
    this.current_batch = []
    this.current_row = 0
    this.header = dict()
    this.maybe_last_part = empty

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
    stream = this.blob_client.download_blob()
    for chunk in stream.chunks():
        this.current += chunk
        this.current_chunk += 1
        extract_rows()
    # output the rest
    if this.maybe_last_part != empty:
        this.current_row += 1
        export(this.maybe_last_part)

    if len(this.current_batch) > 0:
        emit(this.current_batch)
    # we are done
    emit({eof_field: True})


def extract_rows():
    (part, sep, rest) = this.current.partition(this.line_separator)
    # not the last part
    this.maybe_last_part = empty

    if sep != empty:
        if part == empty:
            this.current = rest
            # return row, rest
        else:
            this.current_row += 1
            export(part)
            this.current = rest
            extract_rows()
    else:
        this.maybe_last_part = part


def export(row):
    converted = None
    if this.line_format == format_csv:
        converted = handle_csv(row)
    else:
        if this.line_format == format_json:
            converted = handle_json(row)
        else:
            error('unknown file format ' + this.line_format)

    if converted is not None:
        this.current_batch.append(converted)
        if len(this.current_batch) >= this.batch_size:
            emit(this.current_batch)
            this.current_batch = []


def handle_csv(row):
    if this.current_row == this.header_row:
        this.header = row.split(this.column_seperator)
        emit({header_field: this.header})
        return None
        # print('header', row.split(self.column_seperator))
    else:
        if this.current_row >= this.data_row:
            row_list = row.split(this.column_seperator)
            resdata = dict(zip(this.header, row_list))
            meta = {line_field: this.current_row, chunk_field: this.current_chunk}
            return {meta_dict: meta, data_dict: resdata}
            # res[meta_dict] = meta
            # return res
        else:
            print('skip row', this.current_row)
            return None


def handle_json(row):
    therow = json.loads(row)
    meta = {line_field: this.current_row, chunk_field: this.current_chunk}
    therow[meta_dict] = meta
    return therow


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

