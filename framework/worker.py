""" 
    Provide the Worker Node of the MapReduce Framework 
    Warning: Ports 8080 and 8081 are reserved for the JobTracker(MasterNode) 
"""

from .utils import zmq_addr, msg_deserialize, msg_serialize
from threading import Semaphore, Thread
from os.path import relpath, isdir
from uuid import uuid1
import dill
import zmq

class WorkerNode(object):
    def __init__(self, addr, idle):
        self.addr = addr
        
        self.idle = uuid1().hex

        # predefined master directions
        self.master_pong = zmq_addr(8080)
        self.master_msg = zmq_addr(8081)

        self.zmq_context = zmq.Context()

        self.mapper = None
        self.reducer = None
        self.registered = False

        self.semaphore = Semaphore()

        self.socket = self.zmq_context.socket(zmq.PULL)
        self.socket.bind(zmq_addr(self.addr))
        
        self.paddr = str(int(self.addr) + 1)
        self.socket_pong = self.zmq_context.socket(zmq.PULL)
        self.socket_pong.bind(zmq_addr(self.paddr))

    def __call__(self):
        pong_thread = Thread(target=self.pong_thread, name='pong_thread')
        pong_thread.start()

        while True:
            if not self.registered:
                self.say_hello()

            command, msg = self.socket.recv_serialized(msg_deserialize)
            print(command, msg)

            if command == 'CODE':
                print(f'Worker: {self.addr} -> Receiving CODE from master')
                self.semaphore.acquire()
                self.mapper = msg['mapper']
                self.reducer = msg['reducer']
                self.registered = True
                self.semaphore.release()

            elif command == 'SHUTDOWN':
                break

            elif command == 'TASK':
                task = msg['task']
                func = f'{task.Type}_task'
                res = WorkerNode.__dict__[func](self, task.Body)
                self.send_accomplish(task.Id, res)

            else:
                pass

    def pong_thread(self):
        while True:
            command, msg = self.socket_pong.recv_serialized(msg_deserialize)

            if command == 'PING':
                print('sending pong')
                self.pong()

            elif command == 'kill':
                break

            else:
                pass

    def say_hello(self):
        sock = self.zmq_context.socket(zmq.PUSH)
        sock.connect(self.master_msg)
        sock.send_serialized(['HELLO', {'addr' : self.addr, 'idle' : self.idle }], msg_serialize)
        sock.close()

    def pong(self):
        temp_ctx = zmq.Context()
        sock = temp_ctx.socket(zmq.PUSH)
        sock.connect(self.master_pong)
        sock.send_serialized(['PONG', { 'addr': self.addr }], msg_serialize)
        sock.close()

    def send_accomplish(self, task, response):
        sock = self.zmq_context.socket(zmq.PUSH)
        sock.connect(self.master_msg)
        sock.send_serialized(['DONE', {'task': task, 'response': response}], msg_serialize)
        sock.close()

    def map_task(self, task_body):
        res = [ ]
        chunk = task_body['chunk']
        pairs = self.mapper.parse(chunk)
        for key, value in pairs:
            res += self.mapper.map(key, value)
        res = self.mapper.groupby(res)
        return { 'ikeys': res, 'addr': self.addr }

    def reduce_task(self, task_body):
        partition = task_body['partition']
        res = [ ]
        for ikey, values in partition:
            res.append((ikey, self.reducer.reduce(ikey, values)))
        output_folder = task_body['output_folder']
        assert isdir(output_folder), 'The directory don\'t exist'
        name = f'{relpath(output_folder)}/{self.addr}'
        f = open(name, 'w')
        f.writelines('\n'.join(f'{ikey}-{val}' for ikey, val in res))
        return { 'addr': self.addr }