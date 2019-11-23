from .utils import zmq_addr, msg_deserialize, msg_serialize
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
        # here start thread for incoming message from workers
        msg_thr = Thread(target=self.msg_thread, name="msg_thread")
        msg_thr.start() 

        print('------------------- MAPPING --------------------')

        # send all map task
        while M:
            if self.map_task():
                M -= 1

        print('<-- ALL MAP TASKS SENDED -->')
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

    def msg_thread(self):
        while True: #listen messages forever
            command, msg = self.socket_msg.recv_serialized(msg_deserialize)
            
            if command == 'HELLO':
                worker = Worker(msg['idle'], msg['addr'])
                self.semaphore.acquire()
                self.scheduler.register_worker(worker, msg['idle'])
                self.semaphore.release()
                self.send_code(worker)

            elif command == 'DONE':
                task = msg['task']
                resp = msg['response']
                self.semaphore.acquire()
                self.scheduler.submit_task(task, resp)
                self.semaphore.release()

            else:
                # report error
                print(command)

    def send_code(self, new_worker):
        sock = self.zmq_context.socket(zmq.PUSH)
        sock.connect(zmq_addr(new_worker.addr))
        sock.send_serialized(['CODE', { 'mapper' : self.config.mapper, 'reducer' : self.config.reducer }], msg_serialize)
        sock.close()

    def send_task(self, worker, task):
        sock = self.zmq_context.socket(zmq.PUSH)
        sock.connect(zmq_addr(worker.addr))
        sock.send_serialized(['TASK', {'task': task }])
        sock.close()