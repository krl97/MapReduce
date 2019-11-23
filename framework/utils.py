from more_itertools import chunked
import dill

def zmq_addr(port, transport=None, host=None):
    if host is None:
        host = '127.0.0.1'

    if transport is None:
        transport = 'tcp'

    return f'{transport}://{host}:{port}'

def chunks(file, size):
    f = open(file, 'r')
    lines = f.readlines()
    chs = chunked(lines, size)
    return enumerate(chs)

def msg_serialize(objects: list):
    return [dill.dumps(obj) for obj in objects]

def msg_deserialize(frames: list):
    return tuple(dill.loads(frame) for frame in frames)