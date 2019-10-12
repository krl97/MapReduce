# MapReduce System

MapReduce is a programming model and an associated implementation for processing and generating large data sets.
Programs written in this functional style are automatically parallelized and executed on a large cluster of commodity machines.
The run-time system takes care of the details of partitioning the input data, scheduling the program’s execution across a set of machines, handling machine failures, and managing the required inter-machine communication.

## Especifications

A MapReduce System has two important funtions

 - ```Map``` -> maps and filters a set of data represented by the key-value pair
 - ```Reduce``` -> process the set of values associated to a single _outkey_
 
 A example of map and reduce function using in a word count problem:
 
 ```
 def map(doc_line: int, doc_line_text: str): 
    res = []
    for word in doc_line_text.split(): 
        res.append((word, 1))
    return res
 def reduce(word: str, vals: list): 
    count = 0
    for v in vals:
        count += v
    return count 
 ```
