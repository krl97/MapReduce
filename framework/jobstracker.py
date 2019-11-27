from .utils import zmq_addr, msg_deserialize, msg_serialize
from .scheduler import Scheduler, Worker, JTask
from threading import Thread, Semaphore
import dill
import os
import time
import zmq

class MasterNode(object):
    def __init__(self):
        self.addr_pong = zmq_addr(8080) # in desused
        self.addr_msg = zmq_addr(8081)
        self.addr_submit = zmq_addr(5000)
        
        self.zmq_context = zmq.Context() 

        self.socket_pong = self.zmq_context.socket(zmq.PULL)
        self.socket_pong.bind(self.addr_pong)
        self.socket_msg = self.zmq_context.socket(zmq.PULL)
        self.socket_msg.bind(self.addr_msg)
        self.socket_submit = self.zmq_context.socket(zmq.PULL)
        self.socket_submit.bind(self.addr_submit)

        self.config = None # at moment a only config class... must be more with jobs
        self.semaphore = Semaphore()

        self.scheduler = Scheduler()
        
        self.results = [ ]

    def __call__(self):
        # here start thread for incoming message from workers
        msg_thr = Thread(target=self.msg_thread, name="msg_thread")
        msg_thr.start() 

        while True:
            #TODO: Make a better comunication channel with clients
            config = self.socket_submit.recv_serialized(msg_deserialize)
            config = config[0]
            self.scheduler.submit_job(config.input, config.chunk_size, config.output_folder)
            self.config = config

            print('-- STARTING --')

            states = [self.scheduler.init_map, self.scheduler.init_reduce]

            # send all map task
            while True:
                self.semaphore.acquire()

                #first check if all tasks are done
                if self.scheduler.tasks_done:
                    if not states:
                        self.semaphore.release()
                        break
                    
                    # pass to next state using the init_<state> function
                    next_op = states.pop(0)
                    next_op()

                    self.semaphore.release()
                    continue

                next_task = self.scheduler.next_task()

                if next_task:
                    worker, task = next_task
                    self.send_task(worker, task)

                self.semaphore.release()            

            print('--- DONE: show folder test --- ')

            self.scheduler._reset_tasks() 

        self.shutdown_cluster()
        
    def shutdown_cluster(self):
        sock = self.zmq_context.socket(zmq.PUSH)
        sock.connect(self.addr_msg)
        sock.send_serialized(['kill', None], msg_serialize)

        for w in self.scheduler.workers.keys():
            sock = self.zmq_context.socket(zmq.PUSH)
            sock.connect(zmq_addr(w.Addr))
            sock.send_serialized(['SHUTDOWN', None], msg_serialize)

    def msg_thread(self):
        while True: #listen messages forever
            command, msg = self.socket_msg.recv_serialized(msg_deserialize)
            
            if command == 'HELLO':
                #TODO: Make another register method for workers
                worker = Worker(msg['idle'], msg['addr'])
                self.send_code(worker)
                self.semaphore.acquire()
                self.scheduler.register_worker(worker, msg['idle'])
                self.semaphore.release()

            elif command == 'DONE':
                task = msg['task']
                resp = msg['response']
                self.semaphore.acquire()
                self.scheduler.submit_task(task, resp)
                self.semaphore.release()

            elif command == 'kill':
                break

            else:
                # report error
                print(command)

    def send_code(self, new_worker):
        sock = self.zmq_context.socket(zmq.PUSH)
        sock.connect(zmq_addr(new_worker.Addr))
        sock.send_serialized(['CODE', { 'mapper' : self.config.mapper, 'reducer' : self.config.reducer }], msg_serialize)
        print(f'SENDEND to {new_worker.Addr}')
        sock.close()

    def send_task(self, worker, task):
        sock = self.zmq_context.socket(zmq.PUSH)
        sock.connect(zmq_addr(worker.Addr))
        sock.send_serialized(['TASK', {'task': task }], msg_serialize)
        sock.close()