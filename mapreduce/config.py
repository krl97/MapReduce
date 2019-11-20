""" Provide Configuration Classes for the MapReduce Client """

from .tools.parsing import base_parsing
from .tools.groupby import identity_groupby

import dill

class MapReduce(object):
    """ Defines a MapReduce job configuration class composed by a Mapper configuration 
    class, Reducer configuration class and other configuration parameters """

    def __init__(self, input, mapper, reducer, output_folder, chunk_size = 5):
        self.mapper = dill.dumps(mapper)
        self.reducer = dill.dumps(reducer)
        self.input = input
        self.output_folder = output_folder
        self.chunk_size = chunk_size
        
class Mapper(object):
    """ Defines a base Mapper configuration class. All Mapper configuration class 
    must be inherits from this class and redefines the function map from the
    base class, the parse function must be redefined if the input file don't contains
    the default format for input data, optionally can be redefined the function groupBy 
    for local grouping """

    def __init__(self):
        pass

    def map(self, key, value):
        pass

    def parse(self, data):
        return base_parsing(data)

    def groupby(self, collection):
        return identity_groupby(collection)

class Reducer(object):
    """ Defines a base Reducer configuration class. All Reducer configuration class 
    must be inherits from this class and redefines the function reduce from this base class """
    
    def __init__(self):
        pass

    def reduce(self, key, values):
        pass