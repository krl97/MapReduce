# MapReduce System

MapReduce is a programming model and an associated implementation for processing and generating large data sets.
Programs written in this functional style are automatically parallelized and executed on a large cluster of commodity machines.
The run-time system takes care of the details of partitioning the input data, scheduling the program’s execution across a set of machines, handling machine failures, and managing the required inter-machine communication.

## Process Folder

The process folder contains a MapReduce implementation using multiprocessing to compute map task and reduce task, this 
implementation do not contains a definition for partitions 

## Specifications

A MapReduce System has two important functions

- ```Map``` -> maps and filters a set of data represented by thekey-value pair

- ```Reduce``` -> process the set of values associated to a single_outkey_

A example of map and reduce function using in a word count problem:

```python
def map(doc_line: int, doc_line_text: str):
   res = []
   for word in doc_line_text.split(' '):
       res.append((word, 1))
   return res

def reduce(word: str, vals: list):
   count = 0
   for v in vals:
       count += int(v)
   return count
```

Can be defined other function in the map operation (```Parse``` and ```GroupBy```) for parsing input data and local grouping in workers

## Modules ```mapreduce``` and ```framework```

    Temporal Documentation and project structure

The MapReduce System count with the modules ```mapreduce``` and ```framework```. The ```mapreduce``` module provides the Configuration Class for the MapReduce Job, the classes ```Mapper``` and ```Reducer``` are base class, you must inherits and redefine the functions ```map``` and ```reduce``` in each class, optionally you can define new
```parse``` and ```groupby``` functions with the same signature

Example:

```python
from mapreduce.config import Mapper, Reducer

class WC_Mapper(Mapper):
    def map(self, key, value):
        res = []
        for word in value.split(' '):
            res.append((word, 1))
        return res

class WC_Reducer(Reducer):
    def reduce(self, key, value):
        res = 0
        for elem in value:
            res += int(elem)
        return res
```

The module ```framework``` contains the ```MasterNode``` and ```WorkerNode``` used for the ```server.py``` and ```client.py``` respectively to simulate a cluster of machines

## Running a Example

The scripts ```server.py``` and ```client.py``` contains a example of Word Count, for run this first in 4 terminals run the clients in the ports ```[8082, 8083, 8084, 8085]``` and then run the server and the output file will be in the ```test``` folder