from more_itertools import chunked
from hashlib import sha256
from PyQt5.QtNetwork import QNetworkInterface
import dill

def zmq_addr(port, transport=None, host=None):
    if host is None:
        host = '127.0.0.1'

    if transport is None:
        transport = 'tcp'

    return f'{transport}://{host}:{port}'

def get_host_ip():
    address = QNetworkInterface.allAddresses()
    ip = address[2].toString()
    return ip

def str_hash(s: str) -> int:
    h = sha256(s.encode())
    hexag = h.hexdigest()
    return int(hexag, base = 16)

def chunks(file, size):
    f = open(file, 'r')
    lines = f.readlines()
    f.close()
    chs = list(chunked(lines, size))
    return len(chs), enumerate(chs)

def msg_serialize(objects: list):
    return [dill.dumps(obj) for obj in objects]

def msg_deserialize(frames: list):
    return tuple(dill.loads(frame) for frame in frames)