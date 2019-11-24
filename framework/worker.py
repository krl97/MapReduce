""" 
    Provide the Worker Node of the MapReduce Framework 
    Warning: Ports 8080 and 8081 are reserved for the JobTracker(MasterNode) 
"""

from .utils import zmq_addr, msg_deserialize, msg_serialize
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

        self.socket = self.zmq_context.socket(zmq.PULL)
        self.socket.bind(zmq_addr(self.addr))

    def __call__(self):
        while True:
            if not self.registered:
                self.say_hello()

            command, msg = self.socket.recv_serialized(msg_deserialize)
            
            if command == 'CODE':
                print('Receiving CODE from master')
                self.mapper = msg['mapper']
                self.reducer = msg['reducer']
                self.registered = True

            elif command == 'SHUTDOWN':
                break

            elif command == 'TASK':
                task = msg['task']
                func = f'{task.Type}_task'
                res = WorkerNode.__dict__[func](self, task.Body)
                self.send_accomplish(task.Id, { 'ikeys': res })
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

    def map_task(self, task_body):
        res = [ ]
        chunk = task_body['chunk']
        pairs = self.mapper.parse(chunk)
        for key, value in pairs:
            res += self.mapper.map(key, value)
        res = self.mapper.groupby(res)
        self.map_buffer += res
        return set([key for key, val in res])
        
    def reduce_task(self):
        pass