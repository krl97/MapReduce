""" 
    Provide the Worker Node of the MapReduce Framework 
    Warning: Ports 8080 and 8081 are reserved for the JobTracker(MasterNode) 
"""

from .utils import zmq_addr, msg_deserialize, msg_serialize, get_host_ip, do_broadcast
from threading import Semaphore, Thread
from os.path import relpath, isdir
from uuid import uuid1
import time
import dill
import zmq

class WorkerNode(object):
    """ 8082 -> msg | 8083 -> paddr """
    def __init__(self):
        self.host = get_host_ip()
        self.addr = zmq_addr(8082, host=self.host)
        self.paddr = zmq_addr(8083, host=self.host)

        self.idle = uuid1().hex

        #get master address using broadcast
        master_ip = do_broadcast(self.host, 6666)

        if not master_ip:
            raise Exception('Master not found in the network')

        self.master_pong = zmq_addr(8080, host=master_ip)
        self.master_msg = zmq_addr(8081, host=master_ip)

        self.zmq_context = zmq.Context()

        self.registered = False

        self.semaphore = Semaphore()

        self.socket = self.zmq_context.socket(zmq.PULL)
        self.socket.bind(self.addr)
        
        self.socket_pong = self.zmq_context.socket(zmq.PULL)
        self.socket_pong.bind(self.paddr)

    def __call__(self):
        pong_thread = Thread(target=self.pong_thread, name='pong_thread')
        pong_thread.start()

        while True:
            if not self.registered:
                self.say_hello()

            command, msg = self.socket.recv_serialized(msg_deserialize)
            
            if command == 'REPLY':
                print(f'Worker: {self.addr} -> Receiving REPLY from master')
                self.semaphore.acquire()
                self.registered = True
                self.semaphore.release()

            elif command == 'NEW_MASTER':
                host = msg['host']
                self.master_msg = zmq_addr(8081, host=host)
                self.master_pong = zmq_addr(8080, host=host)
                
            elif command == 'SHUTDOWN':
                break

            elif command == 'TASK':
                task = msg['task']
                print('TASK:', 'TYPE ->', task.Type, ' ID ->', task.Id)
                func = f'{task.Type}_task'
                res = WorkerNode.__dict__[func](self, task.Body, task.Id)
                self.send_accomplish(task.Id, res)

            else:
                pass

    def pong_thread(self):
        while True:
            command, msg = self.socket_pong.recv_serialized(msg_deserialize)

            if command == 'PING':
                self.pong()

            elif command == 'kill':
                break

            else:
                pass

    def say_hello(self):
        sock = self.zmq_context.socket(zmq.PUSH)
        sock.connect(self.master_msg)
        sock.send_serialized(['HELLO', {'addr' : self.addr, 'paddr': self.paddr, 'idle' : self.idle }], msg_serialize)
        sock.close()

    def pong(self):
        sock = self.zmq_context.socket(zmq.PUSH)
        sock.connect(self.master_pong)
        sock.send_serialized(['PONG', {'addr': self.addr}], msg_serialize)
        sock.close()

    def send_accomplish(self, task, response):
        sock = self.zmq_context.socket(zmq.PUSH)
        sock.connect(self.master_msg)
        sock.send_serialized(['DONE', {'task': task, 'response': response}], msg_serialize)
        print('TASK DONE')
        sock.close()

    def map_task(self, task_body, task_id):
        res = [ ]
        chunk = task_body['chunk']
        mapper = task_body['mapper']
        pairs = mapper.parse(chunk)
        for key, value in pairs:
            res += mapper.map(key, value)
        res = mapper.groupby(res)
        return { 'ikeys': res, 'addr': self.addr }

    def reduce_task(self, task_body, task_id):
        partition = task_body['partition']
        reducer = task_body['reducer']
        res = [ ]
        for ikey, values in partition:
            res.append((ikey, reducer.reduce(ikey, values)))
        return { 'output': res, 'addr': self.addr, 'filename': task_id }