#!/usr/bin/env python

import argparse
import csv
import json
import logging
from messytables import (any_tableset, headers_guess, headers_processor,
                         offset_processor, type_guess, types_processor)
from messytables import (StringType, IntegerType, DecimalType, FloatType)
import myria
import StringIO
from struct import Struct

# Set the log level here
logging.getLogger().setLevel(logging.INFO)


def parse_args():
    """Parse the arguments for this program"""
    parser = argparse.ArgumentParser(description='Upload a plaintext dataset to Myria',  # noqa
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)   # noqa

    # Hostname
    parser.add_argument('--hostname', '-n', help="Myria REST server hostname",
                        default='rest.myria.cs.washington.edu')

    # Port
    def check_valid_port(p):
        "True if p is a valid port number"
        try:
            p = int(p)
            assert p > 0 and p < 65536
            return p
        except:
            raise argparse.ArgumentTypeError('invalid port [1, 65535]: %s' % p)

    parser.add_argument('--port', '-p', help="Myria REST server port",
                        default=1776, type=check_valid_port)

    parser.add_argument('file', help="File to be uploaded",
                        type=argparse.FileType('rb'))

    parser.add_argument('--user', help="User who owns the relation",
                        type=str, default="public")
    parser.add_argument('--program', help="Program that owns the relation",
                        type=str, default="adhoc")
    parser.add_argument('--relation', help="Relation name",
                        type=str, required=True)

    parser.add_argument('--overwrite', '-o', help="Overwrite existing data",
                        action='store_true')

    return parser.parse_args()


def convert_type(type_):
    "Convert a MessyTables type to a Myria type."
    if isinstance(type_, StringType):
        return "STRING_TYPE"
    elif isinstance(type_, IntegerType):
        return "LONG_TYPE"
    elif isinstance(type_, [DecimalType, FloatType]):
        return "DOUBLE_TYPE"
    else:
        raise NotImplementedError("type {} is not supported".format(type_))


def messy_to_schema(types, headers=None):
    "Convert a MessyTables schema to a Myria Schema"
    types = [convert_type(t) for t in types]
    if not headers:
        headers = ["column{}".format(i) for i in range(len(types))]
    else:
        headers = [str(t) for t in headers]
    assert len(headers) == len(types)
    logging.info("Schema = {}".format(zip(types, headers)))
    return {'columnTypes': types, 'columnNames': headers}


def args_to_relation_key(args):
    "return the Myria relation key"
    logging.info("RelationKey = {}:{}:{}".format(args.user,
                                                 args.program,
                                                 args.relation))
    return {'userName': args.user,
            'programName': args.program,
            'relationName': args.relation}


def type_fmt(type_):
    "Return the Python struct marker for the type"
    if type_ == 'INT_TYPE':
        return 'i'
    elif type_ == 'LONG_TYPE':
        return 'q'
    elif type_ == 'FLOAT_TYPE':
        return 'f'
    elif type_ == 'DOUBLE_TYPE':
        return 'd'
    raise NotImplementedError('type {} is not supported'.format(type_))


def binary_data(row_set, schema):
    column_types = schema['columnTypes']
    desc = '<' + ''.join(type_fmt(type_) for type_ in column_types)
    struct = Struct(desc)
    logging.info("Creating a binary file with struct.fmt={}".format(desc))
    output = StringIO.StringIO()
    for row in row_set:
        vals = [cell.value for cell in row]
        output.write(struct.pack(*vals))
    return output.getvalue()


def plaintext_data(row_set, schema):
    logging.info("Creating a plaintext file")
    output = StringIO.StringIO()
    writer = csv.writer(output)
    for row in row_set:
        writer.writerow([r.value for r in row])
    return output.getvalue()


def main():
    args = parse_args()

    relation_key = args_to_relation_key(args)

    table_set = any_tableset(args.file)
    if len(table_set.tables) != 1:
        raise ValueError("Can only handle files with a single table, not %"
                         % len(table_set.tables))

    row_set = table_set.tables[0]

    # guess header names and the offset of the header:
    offset, headers = headers_guess(row_set.sample)
    row_set.register_processor(headers_processor(headers))
    # Temporarily, mark the offset of the header
    row_set.register_processor(offset_processor(offset + 1))

    # guess types and register them
    types = type_guess(row_set.sample, strict=True)
    row_set.register_processor(types_processor(types))

    # Messytables seems to not handle the case where there are no headers.
    # Work around this as follows:
    # 1) offset must be 0
    # 2) if the types of the data match the headers, assume there are
    #    actually no headers
    no_headers = False
    if offset == 0:
        try:
            vals = [t.cast(v) for (t, v) in zip(types, headers)]
            no_headers = True
        except:
            pass
    if no_headers:
        # We don't need the headers_processor or the offset_processor
        row_set._processors = []
        row_set.register_processor(types_processor(types))
        headers = None

    # Construct the Myria schema
    schema = messy_to_schema(types, headers)
    logging.info("Myria schema: {}".format(json.dumps(schema)))

    # Connect to Myria
    # connection = myria.MyriaConnection(hostname=args.hostname, port=args.port)

    # Common parameters
    params = [('relationKey', relation_key),
              ('schema', schema),
              ('overwrite', args.overwrite)]

    try:
        data = binary_data(row_set, schema)
        params += [('binary', True), ('isLittleEndian', True)]
        data_type = 'application/octet-stream'
    except:
        data = plaintext_data(row_set, schema)
        data_type = 'text/csv'

    assert not any(name == 'data' for (name, value) in params)
    fields = [(name, (name, json.dumps(value), 'application/json'))
              for (name, value) in params
              if name != 'data']

    # data must be last
    fields.append(('data', ('data', data, data_type)))

    from requests_toolbelt import MultipartEncoder
    import requests
    m = MultipartEncoder(fields=fields)
    r = requests.post('http://{host}:{port}/dataset'.format(host=args.hostname,
                                                            port=args.port),
                      data=m, headers={'Content-Type': m.content_type})
    r.raise_for_status()
    print r.json()

if __name__ == "__main__":
    main()
