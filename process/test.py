from pyMapReduce import MapReduce

def map(key, val):
    for w in val:
        MapReduce.EmitIntermediate(w, 1)
    
def reduce(key, values):
    res = 0
    for c in values:
        res += int(c)
    return res

def main():
    mapreduce = MapReduce(map, reduce)
    mapreduce('input')

if __name__ == "__main__":
    main()
