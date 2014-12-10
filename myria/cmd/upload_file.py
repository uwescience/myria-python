#!/usr/bin/env python

import argparse
import json
import logging
import locale
import sys
import cStringIO
from struct import Struct

import unicodecsv as csv
from messytables import (any_tableset, headers_guess, headers_processor,
                         offset_processor, type_guess, types_processor)
from messytables import (StringType, IntegerType, DecimalType)

import myria

# Set the log level here
logging.getLogger().setLevel(logging.INFO)


def pretty_json(obj):
    return json.dumps(obj, indent=4, separators=(',', ': '))


def parse_args(argv=None):
    """Parse the arguments for this program"""
    parser = argparse.ArgumentParser(
        description='Upload a plaintext dataset to Myria')

    # Hostname
    parser.add_argument('--hostname', '-n',
                        help="Override Myria REST server hostname",
                        default='rest.myria.cs.washington.edu')

    # Port
    def check_valid_port(p):
        """True if p is a valid port number"""
        try:
            p = int(p)
            assert 0 < p < 65536
            return p
        except:
            raise argparse.ArgumentTypeError('invalid port [1, 65535]: %s' % p)
    parser.add_argument('--port', '-p', help="Override Myria REST server port",
                        default=1776, type=check_valid_port)

    # SSL or not (HTTPS or HTTP)
    parser.add_argument('--ssl', help="Use SSL (HTTPS) (default: SSL)",
                        dest="ssl", action="store_true")
    parser.add_argument('--no-ssl',
                        help="Do not use SSL (HTTP) (default: SSL)",
                        dest="ssl", action="store_false")
    parser.set_defaults(ssl=True)

    parser.add_argument('file', help="File to be uploaded (default: stdin)",
                        nargs='?', type=argparse.FileType('rb'))

    def set_locale(name):
        try:
            locale.setlocale(
                locale.LC_NUMERIC, '{name}.UTF-8'.format(name=name))
        except:
            raise argparse.ArgumentTypeError('invalid locale: %s' % name)
        else:
            return name

    parser.add_argument('--locale', '-l',
                        help="locale to improve number guessing",
                        type=set_locale)

    parser.add_argument('--overwrite', '-o',
                        help="Overwrite existing data (default: False)",
                        action='store_true', default=False)
    parser.add_argument('--dry', '-d', help="Output parsed results to stdout",
                        action='store_true', default=False)

    parser.add_argument('--user',
                        help="User who owns the relation (default:public)",
                        type=str, default="public")
    parser.add_argument('--program',
                        help="Program that owns the relation (default: adhoc)",
                        type=str, default="adhoc")
    parser.add_argument('--relation', help="Relation name",
                        type=str, required=True)

    return parser.parse_args(argv)


def convert_type(type_):
    """Convert a MessyTables type to a Myria type."""
    if isinstance(type_, StringType):
        return "STRING_TYPE"
    elif isinstance(type_, IntegerType):
        return "LONG_TYPE"
    elif isinstance(type_, DecimalType):
        return "DOUBLE_TYPE"


def messy_to_schema(types, headers=None):
    "Convert a MessyTables schema to a Myria Schema"
    types = [convert_type(t) for t in types]
    if not headers:
        headers = ["column{}".format(i) for i in range(len(types))]
    else:
        headers = [str(t).strip() for t in headers]
    assert len(headers) == len(types)
    logging.info("Schema = {}".format(zip(types, headers)))
    return {'columnTypes': types, 'columnNames': headers}


def args_to_relation_key(args):
    """return the Myria relation key"""
    logging.info("RelationKey = {}:{}:{}".format(args.user,
                                                 args.program,
                                                 args.relation))
    return {'userName': args.user,
            'programName': args.program,
            'relationName': args.relation}


def type_fmt(type_):
    """Return the Python struct marker for the type"""
    if type_ == 'INT_TYPE':
        return 'i'
    elif type_ == 'LONG_TYPE':
        return 'q'
    elif type_ == 'FLOAT_TYPE':
        return 'f'
    elif type_ == 'DOUBLE_TYPE':
        return 'd'
    raise NotImplementedError('type {} is not supported'.format(type_))


def write_binary(row_set, schema, output):
    column_types = schema['columnTypes']
    desc = '<' + ''.join(type_fmt(type_) for type_ in column_types)
    logging.info("Creating a binary file with struct.fmt={}".format(desc))

    struct = Struct(desc)
    for row in row_set:
        vals = [cell.value for cell in row]
        output.write(struct.pack(*vals))


def write_plaintext(row_set, output):
    logging.info("Creating a plaintext file")
    writer = csv.writer(output)
    for row in row_set:
        writer.writerow([r.value for r in row])


def write_data(row_set, schema):
    """Given a row_set and schema, return (data, kwargs) for sending
    to Myria."""
    output = cStringIO.StringIO()

    if all(type_ in ['INT_TYPE', 'LONG_TYPE', 'FLOAT_TYPE', 'DOUBLE_TYPE']
           for type_ in schema['columnTypes']):
        # File is binary
        write_binary(row_set, schema, output)
        kwargs = {'binary': True, 'is_little_endian': True}
    else:
        # File is plaintext
        write_plaintext(row_set, output)
        kwargs = {}

    return output.getvalue(), kwargs


def strip_processor():
    """remove spaces around all strings"""
    def apply_replace(row_set, row):
        def replace(cell):
            if isinstance(cell.value, basestring):
                cell.value = cell.value.strip()
            return cell
        return [replace(cell) for cell in row]
    return apply_replace


def replace_empty_string(sample):
    """replace empty strings with a non empty string to force
    type guessing to use string"""
    def replace(cell):
        if cell.value == '':
            cell.value = 'empty_string'
        return cell
    return [[replace(cell) for cell in row] for row in sample]


def main(argv=None):
    args = parse_args(argv)

    if args.file is None:
        # slurp the whole input since there seems to be a bug in messytables
        # which should be able to handle streams but doesn't
        args.file = cStringIO.StringIO(sys.stdin.read())

    relation_key = args_to_relation_key(args)

    table_set = any_tableset(args.file)
    if len(table_set.tables) != 1:
        raise ValueError("Can only handle files with a single table, not %s"
                         % len(table_set.tables))

    row_set = table_set.tables[0]

    # guess header names and the offset of the header:
    offset, headers = headers_guess(row_set.sample)
    row_set.register_processor(strip_processor())
    row_set.register_processor(headers_processor(headers))
    # Temporarily, mark the offset of the header
    row_set.register_processor(offset_processor(offset + 1))

    # guess types and register them
    types = type_guess(replace_empty_string(row_set.sample), strict=True,
                       types=[StringType, DecimalType, IntegerType])
    row_set.register_processor(types_processor(types))

    # Messytables seems to not handle the case where there are no headers.
    # Work around this as follows:
    # 1) offset must be 0
    # 2) if the types of the data match the headers, assume there are
    #    actually no headers
    if offset == 0:
        try:
            [t.cast(v) for (t, v) in zip(types, headers)]
        except:
            pass
        else:
            # We don't need the headers_processor or the offset_processor
            row_set._processors = []
            row_set.register_processor(strip_processor())
            row_set.register_processor(types_processor(types))
            headers = None

    # Construct the Myria schema
    schema = messy_to_schema(types, headers)
    logging.info("Myria schema: {}".format(json.dumps(schema)))

    # Prepare data for writing to Myria
    data, kwargs = write_data(row_set, schema)

    if not args.dry:
        # Connect to Myria and send the data
        connection = myria.MyriaConnection(
            hostname=args.hostname, port=args.port, ssl=args.ssl)
        ret = connection.upload_file(relation_key, schema, data,
                                     args.overwrite, **kwargs)

        sys.stdout.write(pretty_json(ret))
    else:
        sys.stdout.write(data)
