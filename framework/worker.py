""" 
    Provide the Worker Node of the MapReduce Framework 
    Warning: Ports 8080 and 8081 are reserved for the JobTracker(MasterNode) 
"""

from .utils import zmq_addr
import dill
import zmq

class WorkerNode(object):
    def __init__(self, addr, idle):
        self.addr = addr
        
        assert idle > 0 'The worker idle must be positive'      
        self.idle = idle

        # predefined master directions
        self.master_pong = zmq_addr(8080)
        self.master_msg = zmq_addr(8081)

        self.zmq_context = zmq.Context()

        self.mapper = None
        self.reducer = None

        self.socket = self.zmq_context.socket(zmq.PULL)
        self.socket.bind(self.addr)

    def __call__(self):
        while True:
            self.say_hello()
            command, msg = self.socket.recv_multipart()
            
            if command == 'CODE':
                self.mapper = dill.loads(msg['mapper'])
                self.reducer = dill.loads(msg['reducer'])

            elif command == 'SHUTDOWN':
                break

            elif command == 'TASK':
                ...
            
            else:
                pass

            task_id = msg['task']
            task_class = msg['class'] #incoming message contains a str with class code

            if task_id in ['map', 'reduce']:
                print(f'Starting Task {task_id}...')
                res = self.task(task_id, task_class, msg)
                self.send_msg(self.master_msg, res)
                print('Task Ended... waiting for instructions')
            elif task_id == 'shutdown':
                break
            else:
                print('Unknown task :( \n Response a fail submit')

    def say_hello(self):
        sock = self.zmq_context.socket(zmq.PUSH)
        sock.connect(self.master_msg)
        sock.send_multipart('HELLO', {'addr' : self.addr, 'idle' : self.idle })

    def task(self, task_id, task_class, msg):
        task_class = dill.loads(task_class)

        if task_id == 'map':
            # wait from task_class: map, parse and groupBy functions
            # TODO: Register errors and send a message with work incomplete and errors details to master node
            lines = self.get_data(msg['file'], msg['chunk'])
            pairs = task_class.parse(lines)
            map_res = [] 
            for key, value in pairs:
                map_res += task_class.map(key, value)
            map_res = task_class.groupby(map_res)

            size = (msg['chunk'][1] - msg['chunk'][0])
            idx = int(msg['chunk'][0] / size)
            
            # map_res contains final result... write this in the local disk at moment
            w = open(f'./test/map_results/map-{self.idle}-{idx}', 'w')
            w.write('\n'.join([ f'{ikey}-{ival}' for ikey, ival in map_res ]))

            # build message for master
            msg = {
                'status' : 'END',
                'idle' : self.idle,
                'task' : 'map-task',
                'chunk' : idx ,
                'route' : f'./test/map_results/map-{self.idle}-{idx}'
            }

            return msg

        elif task_id == 'reduce':
            res = []
            idata = msg['ikeys']
            for key, it in idata.items():
                res.append((key, task_class.reduce(key, it)))
            
            msg = {
                'status' : 'END',
                'idle' : self.idle,
                'task' : 'reduce-task',
                'partition' : msg['partition'],
                'result' : res
            }

            return msg

        # check bad use line
        print('Oops...')

    def get_data(self, file, chunk):
        f = open(file, 'r')
        lines = f.readlines()
        return lines[slice(*chunk)]