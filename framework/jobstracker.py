from .utils import zmq_addr, msg_deserialize, msg_serialize
from .scheduler import Scheduler, Worker, JTask
from threading import Thread, Semaphore
import dill
import os
import time
import zmq

class MasterNode(object):
    def __init__(self, config):
        self.addr_pong = zmq_addr(8080)
        self.addr_msg = zmq_addr(8081)
        
        self.zmq_context = zmq.Context() 

        self.socket_pong = self.zmq_context.socket(zmq.PULL)
        self.socket_pong.bind(self.addr_pong)
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

        states = [self.scheduler.init_shuffle, self.scheduler.init_reduce]

        # send all map task
        while True:
            self.semaphore.acquire()

            #first check if all tasks are done
            if self.scheduler.tasks_done:
                if not states:
                    break

                next_op = states.pop(0)
                next_op()

                self.semaphore.release()
                continue
                
            next_task = self.scheduler.next_task()
            
            if next_task:
                print(next_task.Type)
                worker, task = next_task
                self.send_task(worker, task)

            self.semaphore.release()            

        print('--- DONE --- ')
        

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
        sock.send_serialized(['TASK', {'task': task }], msg_serialize)
        sock.close()