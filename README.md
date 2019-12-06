# MapReduce System

MapReduce is a programming model and an associated implementation for processing and generating large data sets.
Programs written in this functional style are automatically parallelized and executed on a large cluster of commodity machines.
The run-time system takes care of the details of partitioning the input data, scheduling the program’s execution across a set of machines, handling machine failures, and managing the required inter-machine communication.

## Installation

The project dependencies can be installed using:

```bash
make build
```

And a specific node can be started using:

```bash
make master    # master node
make worker    # worker node
make backup    # backup node
```

If you do not have ```make```, can install its:

```bash
sudo apt-get make
```

## Specifications

A MapReduce System has two important functions

- ```Map``` -> maps and filters a set of data represented by thekey-value pair

- ```Reduce``` -> process the set of values associated to a single _outkey_

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

The MapReduce System count with the modules ```mapreduce``` and ```framework```. The ```mapreduce``` module provides the Configuration Class for the MapReduce Job, the classes ```Mapper``` and ```Reducer``` are base class, you must inherits and redefine the functions ```map``` and ```reduce``` in each class, optionally you can define new
```parse``` and ```groupby``` functions with the same signature, the function ```mapreduce``` contained in the 
module ```mapreduce``` is used for the client to submit a new job in the jobstracker, the next example show a correct use of the framework 

Example (```program.py```):

```python
from mapreduce.config import MapReduce, Mapper, Reducer
from mapreduce import mapreduce
import sys

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

if __name__ == "__main__":
    out = sys.argv[1] # output folder in master
    wc_m = WC_Mapper() # Mapper
    wc_r = WC_Reducer() # Reducer

    # config class
    config = MapReduce('./input', wc_m, wc_r, out) 

    # submit job using the mapreduce function
    mapreduce(config)

```

The module ```framework``` contains the ```MasterNode```, ```BackupNode``` and ```WorkerNode``` theirs conforms the cluster 
structure. Only a ```MasterNode``` can be created for the network, the ```BackupNode``` nodes are used to select a new master when 
master node cannot be founded, a ```WorkerNode``` compute the task and sends the values to the master

A ```MasterNode``` contains a JobsTracker, used to organizes the task and distributes this to workers. The scripts ```master.py```,
```slave.py``` and ```b_master.py``` provides a simple form to init and call this nodes, wake up a new node must be done using this 
scripts