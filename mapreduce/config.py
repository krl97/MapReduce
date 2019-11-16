""" Provide Configuration Classes for the MapReduce Client """

from tools.parsing import base_parsing

class MapReduce(object):
    """ Defines a MapReduce job configuration class composed by a Mapper configuration 
    class, Reducer configuration class and other configuration parameters """

    def __init__(self, input, mapper, reducer, output_folder, parsing_function=base_parsing):
        self.mapper = mapper
        self.reducer = reducer
        self.parsing_function = parsing_function
        self.input = input
        self.output_folder = output_folder
        
class Mapper(object):
    """ Defines a base Mapper configuration class. All Mapper configuration class 
    must be inherits from this class and redefines the function map from this base class """

    def __init__(self):
        pass

    def map(self, key, value):
        pass

class Reducer(object):
    """ Defines a base Reducer configuration class. All Reducer configuration class 
    must be inherits from this class and redefines the function reduce from this base class """
    
    def __init__(self):
        pass

    def reduce(self, key, values):
        pass