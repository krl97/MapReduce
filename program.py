from mapreduce.config import MapReduce, Mapper, Reducer
from mapreduce import mapreduce

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
    wc_m = WC_Mapper()
    wc_r = WC_Reducer()

    config = MapReduce('./input', wc_m, wc_r, './test/')

    mapreduce(config)