from framework import MasterNode
from mapreduce.config import MapReduce, Mapper, Reducer
import dill
import os

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
    workers = ['8082',
               '8083',
               '8084',
               '8085']

    wc_m = WC_Mapper()
    wc_r = WC_Reducer()

    mapper = dill.dumps(wc_m)
    reducer = dill.dumps(wc_r)

    config = MapReduce('./input', mapper, reducer, './test/')

    master = MasterNode(workers, config)

    #actually you must run clients first :(
    master()

    print('Server get control again')