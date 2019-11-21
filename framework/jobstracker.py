from .utils import zmq_addr
from .scheduler import Scheduler, Worker, JTask
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

        self.scheduler = Scheduler(config.input, config.chunk_size)
        
        self.results = [ ]

    def __call__(self):
        #here start thread for incoming message from workers
        msg_thr = Thread(target=self.msg_thread, name="msg_thread")
        msg_thr.start() 

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

    def msg_thread(self):
        while True: #listen messages forever
            command, msg = self.socket_msg.recv_multipart()
            
            if command == 'HELLO':
                worker = Worker(msg['idle'], msg['addr'])
                self.scheduler.register_worker(worker, msg['idle'])
                self.send_code(worker.addr)

            elif command == 'DONE':
                task = msg['task']
                resp = msg['response']
                self.scheduler.submit_task(task, resp)
            
            else:
                # report error
                print(command)
                
    def send_code(self, addr):
        sock = self.zmq_context.socket(zmq.PUSH)
        sock.connect(zmq_addr(addr))
        sock.send_multipart(['CODE', { 'mapper' : self.config.mapper, 'reducer' : self.config.reducer }])

    def send_task(self, worker, data):
        socket_worker = self.zmq_context.socket(zmq.PUSH)
        socket_worker.connect(zmq_addr(worker))
        data = dill.dumps(data)
        socket_worker.send(data)
