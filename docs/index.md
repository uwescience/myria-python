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
  from myria import *

  ## Establish a default connection to Myria

  MyriaRelation.DefaultConnection = MyriaConnection(rest_url='http://demo.myria.cs.washington.edu')

  ## Higher-level interaction via relation and query instances
  query = MyriaQuery.submit(
    """books = load('https://raw.githubusercontent.com/uwescience/myria-python/master/ipnb%20examples/books.csv',
                    csv(schema(name:string, pages:int)));
       longerBooks = [from books where pages > 300 emit name];
       store(longerBooks, LongerBooks);"""

  # Download relation and convert it to JSON
  json = query.to_dict()

  # ... or download to a Pandas Dataframe
  dataframe = query.to_dataframe()

  # ... or download to a Numpy array
  dataframe = query.to_dataframe().as_matrix()

  ## Access an already-stored relation
  relation = MyriaRelation(relation='LongerBooks')
  print len(relation)

  ## Lower-level interaction via the REST API
  connection = MyriaConnection(rest_url='http://demo.myria.cs.washington.edu')
  datasets = connection.datasets()
```

![Myria-Python Workflow](https://raw.githubusercontent.com/uwescience/myria-python/master/ipnb%20examples/overview.png "Myria-Python Workflow")

## Installation

Users can install the Python libraries using `pip install myria-python`. Developers should clone the [repository](https://github.com/uwescience/myria-python) and run `python setup.py develop`.

## Using Python with the Myria Service

### Part 1: Running Queries

In this Python example, we query the smallTable relation by creating a count(*) query using the MyriaL language.  In this query, we store our result to a relation called countResult. To learn more about the Myria query language, check out the [MyriaL](http://myria.cs.washington.edu/docs/myrial.html) page.

```python
from myria import *

connection = MyriaConnection(rest_url='http://demo.myria.cs.washington.edu:8753')

query = MyriaQuery.submit("""
  data = load('https://raw.githubusercontent.com/uwescience/myria/master/jsonQueries/getting_started/smallTable',
              csv(schema(left:int, right:int)));
  q = [from data emit count(*)];
  store(q, dataCount);""", connection=connection)

print query.to_dict()
```

### Part 2: Downloading Data

In the previous example we downloaded the result of a query.  We can also download data that has been stored as a relation:

```python
connection = MyriaConnection(rest_url='http://demo.myria.cs.washington.edu:8753')

# Load some data and store it in Myria
query = MyriaQuery.submit("""
  data = load('https://raw.githubusercontent.com/uwescience/myria/master/jsonQueries/getting_started/smallTable',
              csv(schema(left:int, right:int)));
  store(data, data);""", connection=connection)

# Now access previously-stored data
relation = MyriaRelation('data', connection=connection)

print relation.to_dict()[:5]
```

### Part 3: Uploading Data

#### From a local Python variable

```python
from myria import *

name = {'userName': 'public', 'programName': 'adhoc', 'relationName': 'Books'}
schema = { "columnNames" : ["name", "pages"],
           "columnTypes" : ["STRING_TYPE","LONG_TYPE"] }

data = """Brave New World,288
Nineteen Eighty-Four,376
We,256"""

connection = MyriaConnection(rest_url='http://demo.myria.cs.washington.edu:8753')
result = connection.upload_file(
    name, schema, data, delimiter=',', overwrite=True)

relation = MyriaRelation("Books", connection=connection)
print relation.to_dict()
```

#### From a Local File

```python
import sys
import urllib
import random
from myria import *

connection = MyriaConnection(rest_url='http://demo.myria.cs.washington.edu:8753')

# Download a sample file to our local filesystem
urllib.urlretrieve ("https://raw.githubusercontent.com/uwescience/myria-python/master/ipnb%20examples/books.csv",
                    "books.csv")

# Initialize a name and schema for the new relation
name = {'userName': 'public',
        'programName': 'adhoc',
        'relationName': 'Books' + str(random.randrange(sys.maxint)) } # Name must be unique!
schema = { "columnNames" : ["name", "pages"],
           "columnTypes" : ["STRING_TYPE","LONG_TYPE"] }

# Now upload that file to Myria
with open('books.csv') as f:
    connection.upload_fp(name, schema, f)

# Now access the new relation
relation = MyriaRelation(name, connection=connection)
print relation.to_dict()
```

#### From the Command Line

In the example below, we upload a local CSV file to the Myria Service. Here is an example you can run through your terminal (assuming you've setup myria-python):

```shell
wget https://raw.githubusercontent.com/uwescience/myria/master/jsonQueries/getting_started/smallTable
myria_upload --overwrite --hostname demo.myria.cs.washington.edu --port 8753 --no-ssl --relation smallTable smallTable
```

#### Loading Large Datasets In Parallel

```python
from myria import *

connection = MyriaConnection(rest_url='http://demo.myria.cs.washington.edu:8753')
schema = MyriaSchema({"columnTypes" : ["LONG_TYPE", "LONG_TYPE"], "columnNames" : ["follower", "followee"]})
relation = MyriaRelation('parallelLoad', connection=connection, schema=schema)

# A list of worker-URL pairs -- must be one for each worker
work = [(1, 'https://s3-us-west-2.amazonaws.com/uwdb/sampleData/smallTable'),
        (2, 'https://s3-us-west-2.amazonaws.com/uwdb/sampleData/smallTable'),
        (3, 'https://s3-us-west-2.amazonaws.com/uwdb/sampleData/smallTable')]

# Upload the data
query = MyriaQuery.parallel_import(relation=relation, work=work)
print query.status
```

## Using Myria with IPython

Myriaexposes convenience functionality when running within the Jupyter/IPython environment.  See [our sample IPython notebook](https://github.com/uwescience/myria-python/blob/master/ipnb%20examples/myria%20examples.ipynb) for a live demo.

### Load the Extension

```python
%load_ext myria
```

### Connecting to Myria

```python
%connect http://demo.myria.cs.washington.edu:8753
```

### Executing Queries

```python
%%query
books = load('https://raw.githubusercontent.com/uwescience/myria-python/master/ipnb%20examples/books.csv',
             csv(schema(name:string, pages:int)));
       longerBooks = [from books where pages > 300 emit name];
       store(longerBooks, LongerBooks);
```

### Variable Binding

You can embed local Python variables into a query expression.  For example, assume we have set the following local variables:

```python
low, high, name = 300, 1000, 'MyBooks'
```

Now we can execute a query in an IPython notebook that binds over our local environment:

```python
%%query
books = load('https://raw.githubusercontent.com/uwescience/myria-python/master/ipnb%20examples/books.csv',
             csv(schema(name:string, pages:int)));
       longerBooks = [from books where pages > @low and pages < @high emit name];
       store(longerBooks, @name);
```