from .utils import zmq_addr
from threading import Thread, Semaphore
import dill
import os
import time
import zmq

class MasterNode(object):
    def __init__(self, workers, config):
        self.addr_pong = zmq_addr(8080)
        self.addr_msg = zmq_addr(8081)
        
        self.zmq_context = zmq.Context() 

        self.socket_pong = self.zmq_context.socket(zmq.PULL)
        self.socket_pong.bind(self.addr_task)
        self.socket_msg = self.zmq_context.socket(zmq.PULL)
        self.socket_msg.bind(self.addr_msg)

        self.config = config
        self.semaphore = Semaphore()

        self.workers = { worker : 'non-task' for worker in workers }
        self.map_routes = []
        self.chunk_size = config.chunk_size # number of lines
        
        self.current_chunk = 0
        self.current_partition = 0

        self.M = MasterNode.map_splitter(self.config.input, self.chunk_size)
        self.R = 4

        self.chunks_state = ['pending'] * self.M
        self.partition_state = ['pending'] * self.R

        self.partitions = None

        self.ikeys = { }

        self.results = [ ]

    def ()

    def __call__(self):
        #here start thread for incoming message from workers
        msg_thr = Thread(target=self.msg_thread, name="msg_thread")
        msg_thr.start() 

        M = self.M  #get number of map task to send 

        print('------------------- MAPPING -------------------')

        # send all map task
        while M:
            if self.map_task(): 
                M -= 1

        print('<-- ALL MAP TASKS SENDED --> ')
        print(self.workers)
        
        # wait for map workers
        while True:
            self.semaphore.acquire()
            if all([state == 'completed' for state in self.chunks_state]):
                self.semaphore.release()
                break
            self.semaphore.release()            

        print('-------------------- REDUCE TASKS -------------------')

        # reduceTrue
        self.shuffle()
        self.partitioning()

        R = self.R

        while R:
            if self.reduce_task():
                R -= 1

        while True:
            self.semaphore.acquire()
            if all([state == 'completed' for state in self.partition_state]):
                self.semaphore.release()
                break
            self.semaphore.release()

        # write to output folder

        f = open(f'{self.config.output_folder}/output', 'w')
        f.write('\n'.join([f'{res[0]}-{res[1]}' for res in self.results]))

        print('------------------ END ------------------')
        
        for worker in self.workers:
            self.send_task(worker, {'task': 'shutdown', 'class' : None })

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
        print('Shuffle Success')

    def partitioning(self):
        inter = list(self.ikeys.keys())
        part = [ { } for _ in range(self.R) ]

        for hash, key in enumerate(inter):
            part[hash % self.R][key] = self.ikeys[key]

        self.partitions = part
        print('Partition Success')

    def get_worker(self):
        self.semaphore.acquire()
        res = None
        for worker, state in self.workers.items():
            if state in ['non-task', 'completed']:
                res = worker
                break
        self.semaphore.release()
        return res

    def msg_thread(self):
        while True: #listen messages forever
            msg = self.socket_msg.recv_json()
            
            #parse message
            if msg['status'] == 'END':
                worker = msg['idle']
                task = msg['task']

                if task == 'map-task':
                    chunk_idx = msg['chunk']
                    self.semaphore.acquire()
                    self.workers[worker] = 'completed'
                    self.chunks_state[chunk_idx] = 'completed'
                    self.map_routes.append(msg['route'])
                    self.semaphore.release()
                    print(f'Worker: {worker} end map-task')
                
                elif task == 'reduce-task':
                    partition = msg['partition']
                    self.semaphore.acquire()
                    self.workers[worker] = 'completed'
                    self.partition_state[partition] = 'completed'
                    self.results += msg['result']
                    self.semaphore.release()
                    print(f'Worker: {worker} end reduce-task')

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

            self.send_task(worker, data)

            print(f'SENDED to: {worker}')
            self.semaphore.acquire()
            if self.chunks_state[int(self.current_chunk[0] / self.chunk_size)] != 'completed':
                self.chunks_state[int(self.current_chunk[0] / self.chunk_size)] = 'in-progress'  #set sended chunk
            self.current_chunk = (self.current_chunk[1], self.current_chunk[1] + self.chunk_size)
            self.workers[worker] = 'map-task'
            self.semaphore.release()
            return True

        return False

    def reduce_task(self):
        """ Assign a reduce task """
        worker = self.get_worker()

        if worker:
            # build message
            data = {
                'task': 'reduce',
                'class': self.config.reducer,
                'partition': self.current_partition,
                'ikeys': self.partitions[self.current_partition]
            }

            self.send_task(worker, data)
            self.semaphore.acquire()
            self.partition_state[self.current_partition] = 'in-progress'
            self.current_partition += 1
            self.workers[worker] = 'reduce-task'
            self.semaphore.release()
            return True

        return False
