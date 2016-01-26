---
layout: default
title: Myria Python
group: "docs"
weight: 4
section: 3
---

# Myria Python

Myria-Python is a Python interface to the [Myria project](http://myria.cs.washington.edu), a distributed, shared-nothing big data management system and Cloud service from the [University of Washington](http://www.cs.washington.edu).

The Python components include intuitive, high-level interfaces for working with Myria, along with lower-level operations for interacting directly with the Myria API.

```python
  # Lower-level interaction via the API connection
  connection = MyriaConnection(hostname='demo.myria.cs.washington.edu', port=8753)
  datasets = connection.datasets()

  # Higher-level interaction via relation and query instances
  relation = MyriaRelation(relation='public:adhoc:smallTable', connection=connection)
  json = relation.to_dict()
```

## Installation

Users can install the Python libraries using `pip install myria-python`. Developers should clone the [repository](https://github.com/uwescience/myria-python) and run `python setup.py develop`.


## Using Python with the Myria Service

### Part 1: Uploading Data

We illustrate the basic functionality using examples in the directory
`jsonQueries/getting_started`. The  `jsonQueries` directory contains additional examples. In the example below, we upload the smallTable to the Myria Service. Here is an example you can run through your terminal (assuming you've setup myria-python):

```
myria_upload --overwrite --hostname demo.myria.cs.washington.edu --port 8753 --no-ssl --relation smallTable /path/to/file
```

### Part 2: Running MyriaL Queries
In this Python example, we query the smallTable relation by creating a count(*) query using the MyriaL language.In this query, we store our result to a relation called countResult. To learn more about the Myria query language, check out the [MyriaL](http://myria.cs.washington.edu/docs/myrial.html) page.

```
from myria import MyriaConnection
connection = MyriaConnection(hostname='demo.myria.cs.washington.edu', port=8753)
program = "q = [from scan(public:adhoc:smallTable) as t emit count(*) as countRelation]; store(q, countResult);"
connection.execute_program(program=program, server="http://demo.myria.cs.washington.edu")
```

### Part 3: Downloading Data
Finally, we can download the result of our query by downloading the countResult table through the following Python program:

```
from myria import MyriaConnection, MyriaRelation
connection = MyriaConnection(hostname='demo.myria.cs.washington.edu', port=8753)
relation = MyriaRelation('public:adhoc:smallTable', connection=connection)
data = relation.to_dict()
print data
```

## Using Python with your own Myria Deployment
For the examples below, we used localhost as the hostname example. This can be changed depending on where you are hosting Myria.

### Part 1: Uploading Data
```
from myria import MyriaConnection
connection = MyriaConnection(hostname='localhost', port='8753')
relation = {"userName": "jwang", "programName": "global_join", "relationName": "smallTable"}
schema = {"columnTypes" : ["LONG_TYPE", "LONG_TYPE"], "columnNames" : ["follower", "followee"]}
source = {"dataType" : "File", "filename" : "/path/to/file"}
response = connection.upload_source(relation_key=relation, schema=schema, source=source)
```

Alternatively, you can upload data through the myira-upload tool:

```
myria_upload --hostname localhost --port 8753 --no-ssl --user jwang --program global_join --relation smallTable /path/to/file
```

### Part 2: Building Queries
We can run a json query by running the following program:

```
from myria import MyriaConnection
connection = MyriaConnection(hostname='localhost', port='8753')
connection.submit_query(query="/path/to/json/query")
```

### Part 3: Downloading Data
Finally, we can download the result of our query from Part 2 by running the following Python program:

```
from myria import MyriaConnection, MyriaRelation
connection = MyriaConnection(hostname='localhost', port=8753)
relation = MyriaRelation('jwang:global_join:smallTable_join_smallTable', connection=connection)
data = relation.to_dict()
print data
```

## Loading Datasets in Parallel
```
from myria import MyriaConnection, MyriaRelation, MyriaQuery, MyriaSchema

connection = MyriaConnection(hostname='demo.myria.cs.washington.edu', port=8753)
schema = MyriaSchema({"columnTypes" : ["LONG_TYPE", "LONG_TYPE"], "columnNames" : ["follower", "followee"]})
relation = MyriaRelation('public:adhoc:parallelLoadTest', connection=connection, schema=schema)

work = [(1, 'https://s3-us-west-2.amazonaws.com/uwdb/sampleData/smallTable'),
        (2, 'https://s3-us-west-2.amazonaws.com/uwdb/sampleData/smallTable'),
        (3, 'https://s3-us-west-2.amazonaws.com/uwdb/sampleData/smallTable'),
        (4, 'https://s3-us-west-2.amazonaws.com/uwdb/sampleData/smallTable')]

queryImport = MyriaQuery.parallel_import(relation=relation, work=work)
```
