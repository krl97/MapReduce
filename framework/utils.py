from more_itertools import chunked
from hashlib import sha256
from PyQt5.QtNetwork import QNetworkInterface
import dill
import socket
import random

def zmq_addr(port, transport=None, host=None):
    if host is None:
        host = '127.0.1.1'

    if transport is None:
        transport = 'tcp'

    return f'{transport}://{host}:{port}'

def waiting_to_broadcast(ip, port):
    client = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)  # UDP
    client.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    client.bind(('', port))

    while True:
        data, addr = client.recvfrom(1024)
        print(addr)
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        while True:
            try:
                sock.connect((addr[0], addr[1]))
                break
            except (ConnectionRefusedError, OSError):
                print("Can't connect to " + str((addr[0], addr[1])))
                #continue
                break
        try:
            sock.send(ip.encode())
        except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError, ConnectionError, ConnectionRefusedError):
            continue

    client.close()

def do_broadcast(ip, port):
    # Initialize list_uri
    master_ip = None
    # Sending broadcast
    client = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    client.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    # Set a timeout so the socket does not block
    # indefinitely when trying to receive data.
    # server.set timeout(0.2)

    rport = None
    while True:
        rport = random.randint(3000, 4000)
        try :
            client.bind((ip, rport))
            break
        except OSError :
            pass

    message = b'hi, i\'m new here'
    
    for i in range(10): 
        if i > 0:
            client = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
            client.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
            client.bind((ip, rport))
        client.sendto(message, ('<broadcast>', port))
        client.close()

        # Then we listen in a TCP socket
        # Listening response
        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        # Set timeout
        try:
            client.bind((ip, rport))
        except OSError:
            return

        client.settimeout(3)
        client.listen(1000)
        # Fill list_uir with the responses
        try:
            s, addr_info = client.accept()
            recv_ip = s.recv(1024).decode()
            if recv_ip != '':
                master_ip = recv_ip 
                return master_ip

        except socket.timeout:
            print('RETRY #:', i)
            client.close()
            continue
    
    return None

def get_host_ip(lh = False):
    if lh:
        return '127.0.1.1'
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