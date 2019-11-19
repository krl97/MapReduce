from .utils import zmq_addr
from threading import Thread, Semaphore
import dill
import zmq

class MasterNode(object):
    def __init__(self, workers, config):
        self.addr_task = zmq_addr(8080)
        self.addr_msg = zmq_addr(8081)
        
        self.zmq_context = zmq.Context() 

        self.socket_task = self.zmq_context.socket(zmq.PULL)
        self.socket_task.bind(self.addr_task)
        self.socket_msg = self.zmq_context.socket(zmq.PULL)
        self.socket_msg.bind(self.addr_msg)

        self.config = config
        self.semaphore = Semaphore()

        self.workers = { worker : 'non-task' for worker in workers }
        self.map_routes = []
        self.chunk_size = config.chunk_size # number of lines
        self.current_chunk = (0, self.chunk_size)
        self.M = MasterNode.map_splitter(self.config.input, self.chunk_size)
        self.chunks_state = ['pending'] * self.M
        self.ikeys = { }

    def __call__(self):
        #here start thread for incoming message from workers
        msg_thr = Thread(target=self.msg_thread, name="msg_thread")
        msg_thr.start() 

        M = self.M  #get number of map task to send 

        # send all map task
        while M:
            if self.map_task(): 
                M -= 1

        print('-- HERE -- ', M)
        print(self.chunks_state)
        print(self.workers)
        print(self.map_routes)

        # wait for map workers
        while True:
            if all([state == 'completed' for state in self.chunks_state]):
                break

        print(self.chunks_state)
        print(self.workers)
        print('-- END --')

        # reduce


    def shuffle(self):
        for map_file in self.map_routes:
            f = open(map_file)
            keys = f.readlines()
            inters = [tuple(line.split('-')) for line in keys]
            for k, v in inters:
                try:
                    self.ikeys[k].append(v)
                except:
                    self.ikeys[k] = [ v ]


    @staticmethod
    def map_splitter(file, size):
        f = open(file, 'r')
        lines = len(f.readlines())
        return int(lines / size) + (lines % size > 0)

    def get_worker(self):
        self.semaphore.acquire()
        res = None
        for worker, state in self.workers.items():
            if state in ['non-task', 'completed']:
        def shuffle():
                res = worker
                break
        self.semaphore.release()
        return res

    def msg_thread(self):
        while True: #listen messages forever
            print('T-Waiting')
            msg = self.socket_msg.recv_json()
            print('T-Process:', msg)

            #parse message
            if msg['status'] == 'END':
                worker = msg['idle']
                task = msg['task']
                chunk_idx = msg['chunk']

                if task == 'map-task':
                    self.semaphore.acquire()
                    self.workers[worker] = 'completed'
                    self.chunks_state[chunk_idx] = 'completed'
                    self.map_routes.append(msg['route'])
                    self.semaphore.release()
                    print(f'Worker: {worker} end map-task')
                else:
                    #other task
                    print(f'Worker talking about: {task}')

            else:
                # report error
                print(msg['status'])

    def send_task(self, worker, data):
        socket_worker = self.zmq_context.socket(zmq.PUSH)
        socket_worker.connect(zmq_addr(worker))
        data = dill.dumps(data)
        socket_worker.send(data)
        
        #TODO: add timeout for waiting
        #reply = self.socket_task.recv_json()  
        #return reply   
        return {'status' : 'RECV'}

    def map_task(self):
        """  Assign a map task """
        worker = self.get_worker()

        if worker:
            # build the data
            data = {'task' : 'map',
                    'class' : self.config.mapper, #serialized with dill ( this maybe come with a parsing function and local group function)
                    'file' : self.config.input,
                    'chunk' : self.current_chunk 
                }

            reply = self.send_task(worker, data)

            if reply['status'] == 'RECV':
                print(f'REPLY from {worker}')
                self.semaphore.acquire()
                self.chunks_state[int(self.current_chunk[0] / self.chunk_size)] = 'in-progress' #set sended chunk
                self.current_chunk = (self.current_chunk[1], self.current_chunk[1] + self.chunk_size)
                self.workers[worker] = 'map-task'
                print(self.workers)
                print(self.chunks_state)
                self.semaphore.release()
                return True

        return False

    #TODO: Get values from network 
    def reduce_task(self):
        pass