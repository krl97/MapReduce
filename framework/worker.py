""" 
    Provide the Worker Node of the MapReduce Framework 
    Warning: Ports 8080 and 8081 are reserved for the JobTracker(MasterNode) 
"""

from .utils import zmq_addr
import dill
import zmq

class WorkerNode(object):
    def __init__(self, addr):
        self.addr = zmq_addr(addr)
        self.idle = addr

        # predefined master directions
        self.master_tsk = zmq_addr(8080)
        self.master_msg = zmq_addr(8081)

        self.zmq_context = zmq.Context()

        self.status = 'non-task'

        self.socket = self.zmq_context.socket(zmq.PULL)
        self.socket.bind(self.addr)

    def __call__(self):
        while True:
            msg = self.socket.recv()
            msg = dill.loads(msg)
            task_id = msg['task']
            task_class = msg['class'] #incoming message contains a str with class code

            reply_socket = self.zmq_context.socket(zmq.PUSH)
            reply_socket.connect(self.master_tsk)
            if task_id in ['map', 'reduce']:
                #reply_socket.send_json({'status': 'RECV'})
                print(f'Starting Task {task_id}...')
                res = self.task(task_id, task_class, msg)
                self.send_msg(self.master_msg, res)
                print('Task Ended... waiting for instructions')
            else:
                print('Unknown task :( \n Response a fail submit')
                #reply_socket.send_json({'status' : 'FAIL'})

    def send_msg(self, addr, msg):
        sock_msg = self.zmq_context.socket(zmq.PUSH)
        sock_msg.connect(self.master_msg)
        sock_msg.send_json(msg)

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
            print(idx)

            # map_res contains final result... write this in the local disk at moment
            w = open(f'./test/map_results/map-{self.idle}-{idx}', 'w')
            w.writelines([ f'{ikey}-{ival}\n' for ikey, ival in map_res ])


            #build message for master
            msg = {
                'status' : 'END',
                'idle' : self.idle,
                'task' : 'map-task',
                'chunk' : idx ,
                'route' : f'./map-{self.idle}'
            }

            return msg

        elif task_id == 'reduce':
            pass

        # check bad use line
        print('Oops...')

    def get_data(self, file, chunk):
        f = open(file, 'r')
        lines = f.readlines()
        return lines[slice(*chunk)]