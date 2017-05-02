"""
Given a sqlite3 database file, upload all tables to Myria under user public, program sqlite

Usage:
$ python push_sqlite.py <path/to/sqlite.db> [<user>, <program>]

Type mapping may cause trouble; see typemap function
"""

import sqlite3
import sys
import myria

class filelikeIterator:
  """Wrap a generator object as a file-like object"""
  def __init__( self, it ):
    self.it = it
    self.next_chunk = ""
  def growChunk( self ):
    self.next_chunk = self.next_chunk + self.it.next()
  def read( self, n=-1):
    if self.next_chunk == None:
      return None
    try:
      while len(self.next_chunk)<n:
        self.growChunk()
      rv = self.next_chunk[:n]
      self.next_chunk = self.next_chunk[n:]
      return rv
    except StopIteration:
      rv = self.next_chunk
      self.next_chunk = None
      return rv


connection = myria.MyriaConnection(rest_url='https://rest.myria.cs.washington.edu:1776',
        execution_url='https://myria-web.appspot.com')


def typemap(dbt):
  """translate sqlite3 types to myria types.  Not complete!!"""
  if "VAR" in dbt: 
    return "STRING_TYPE"
  elif "int" in dbt or "long" in dbt:
    return "LONG_TYPE"
  elif "real" in dbt or "double" in dbt:
    return "DOUBLE_TYPE"
  else:
    return dbt

sqlconn = sqlite3.connect(sys.argv[1])


def tuplestream(tblname):
  c = sqlconn.cursor()
  for row in c.execute("SELECT * FROM '{0}'".format(tblname)):
    yield row
  return

tbls = "SELECT tbl_name FROM sqlite_master WHERE type='table' and tbl_name not like 'sqlite_%'"

ct = sqlconn.cursor()
cc = sqlconn.cursor()

for row in ct.execute(tbls):
  tbl = row[0]

  attrs = [(col[1], col[2]) for col in cc.execute("PRAGMA table_info(%s)" % row)]
  names, dbtypes = zip(*attrs)
  mtypes = [typemap(dbt) for dbt in dbtypes]
  schema = {"columnNames": names,
            "columnTypes": mtypes}

  print "uploading table {0} with schema {1}".format(tbl, schema)

  f = filelikeIterator(tuplestream(tbl))
  relkey = {'userName': 'Public', 'programName': 'SQLite', 'relationName': tbl}

  # upload to myria
  connection.upload_fp(relkey, schema, f)
