""" 
    Provide the Worker Node of the MapReduce Framework 
    Warning: Ports 8080 and 8081 are reserved for the JobTracker(MasterNode) 
"""

from .utils import zmq_addr, msg_deserialize, msg_serialize
from threading import Semaphore, Thread
from os.path import relpath, isdir
import dill
import zmq

class WorkerNode(object):
    def __init__(self, addr, idle):
        self.addr = addr
        
        assert idle > 0, 'The worker idle must be positive'      
        self.idle = idle

        # predefined master directions
        self.master_pong = zmq_addr(8080)
        self.master_msg = zmq_addr(8081)

        self.zmq_context = zmq.Context()

        self.mapper = None
        self.reducer = None
        self.registered = False

        self.map_buffer = [ ]
        self.reduce_buffer = { }

        self.semaphore = Semaphore()

        self.socket = self.zmq_context.socket(zmq.PULL)
        self.socket.bind(zmq_addr(self.addr))
        self.sock_recv = self.zmq_context.socket(zmq.PULL)
        self.raddr = str(int(self.addr) + 1) # TODO: IP tests 
        self.sock_recv.bind(zmq_addr(self.raddr))

    def __call__(self):
        thr = Thread(target=self.thr_receive, name='thr_receive')
        thr.start()

        while True:
            if not self.registered:
                self.say_hello()

            command, msg = self.socket.recv_serialized(msg_deserialize)
            
            if command == 'CODE':
                print(f'Worker: {self.addr}-{self.raddr} -> Receiving CODE from master')
                self.semaphore.acquire()
                self.mapper = msg['mapper']
                self.reducer = msg['reducer']
                self.registered = True
                self.semaphore.release()

            elif command == 'SHUTDOWN':
                sock = self.zmq_context.socket(zmq.PUSH)
                sock.connect(zmq_addr(self.raddr))
                sock.send_serialized(['kill', None], msg_serialize)
                break

            elif command == 'TASK':
                task = msg['task']
                func = f'{task.Type}_task'
                res = WorkerNode.__dict__[func](self, task.Body)
                self.send_accomplish(task.Id, res)
            else:
                pass

    def say_hello(self):
        sock = self.zmq_context.socket(zmq.PUSH)
        sock.connect(self.master_msg)
        sock.send_serialized(['HELLO', {'addr' : self.addr, 'idle' : self.idle }], msg_serialize)
        sock.close()

    def send_accomplish(self, task, response):
        sock = self.zmq_context.socket(zmq.PUSH)
        sock.connect(self.master_msg)
        sock.send_serialized(['DONE', {'task': task, 'response': response}], msg_serialize)
        sock.close()

    def send_ikey(self, ikey, value, addr):
        temp_context = zmq.Context()
        sock = temp_context.socket(zmq.PUSH)
        sock.connect(zmq_addr(addr))
        sock.send_serialized(['IKEY', {'ikey': ikey, 'value': value }], msg_serialize)
        sock.close()

    def map_task(self, task_body):
        res = [ ]
        chunk = task_body['chunk']
        pairs = self.mapper.parse(chunk)
        for key, value in pairs:
            res += self.mapper.map(key, value)
        res = self.mapper.groupby(res)
        self.map_buffer += res
        return { 'addr': self.addr, 'raddr': self.raddr }

    def shuffle_task(self, task_body):
        mappers = task_body['mappers']
        print(mappers)
        f_hash = task_body['hash']
        while self.map_buffer:
            ikey, value = self.map_buffer.pop()
            idx = f_hash(ikey)
            addr = mappers[idx]
            if addr == self.raddr:
                self.semaphore.acquire()
                try:   
                    self.reduce_buffer[ikey].append(value)
                except:
                    self.reduce_buffer[ikey] = [ value ]
                self.semaphore.release()
            else:
                self.send_ikey(ikey, value, addr)
        print(f'Worker: {self.addr}-{self.raddr} -> DONE-SHUFFLE')
        return { 'addr': self.addr }

    def reduce_task(self, task_body):
        res = [ ]
        for ikey, values in self.reduce_buffer.items():
            res.append((ikey, self.reducer.reduce(ikey, values)))
        output_folder = task_body['output_folder']
        assert isdir(output_folder), 'The directory don\'t exist'
        name = f'{relpath(output_folder)}/{self.addr}'
        f = open(name, 'w')
        f.writelines('\n'.join(f'{ikey}-{val}' for ikey, val in res))
        return { 'addr': self.addr }

    def thr_receive(self):
        while True:
            command, msg = self.sock_recv.recv_serialized(msg_deserialize)

            if command == 'kill':
                break

            elif command == 'IKEY':
                key = msg['ikey']
                value = msg['value']
                self.semaphore.acquire()
                try:   
                    self.reduce_buffer[key].append(value)
                except:
                    self.reduce_buffer[key] = [ value ]
                self.semaphore.release()

            else:
                print(command)