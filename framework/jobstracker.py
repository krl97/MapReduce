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

        self.semaphore = Semaphore()
        self.backups = [ ]

        self.scheduler = Scheduler()
        
        self.results = [ ]

    def __call__(self):
        # here start thread for incoming message from workers
        msg_thread = Thread(target=self.msg_thread, name="msg_thread")
        ping_thread = Thread(target=self.ping_thread, name="ping_thread")
        msg_thread.start()
        ping_thread.start() 

        print('INIT')

        while True:
            #TODO: Make a better comunication channel with clients
            config = self.socket_submit.recv_serialized(msg_deserialize)
            config = config[0]
            self.scheduler.submit_job(config)

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

                    self.send_scheduler()

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

    def ping_thread(self):
        c = zmq.Context()
        while True:
            self.semaphore.acquire()
            workers = [w for w in list(self.scheduler.workers.keys())]
            self.semaphore.release()

            print(workers)
            
            for worker in workers:
                sock = c.socket(zmq.PUSH)
                sock.connect(zmq_addr(worker.pong_addr))
                sock.send_serialized(['PING', None], msg_serialize, zmq.NOBLOCK)
                
                poller = zmq.Poller()
                poller.register(self.socket_pong, zmq.POLLIN)
            
                sck = dict(poller.poll(1000))
                if sck:
                    command, msg = self.socket_pong.recv_serialized(msg_deserialize, zmq.NOBLOCK)
                    
                    if command == 'PONG' and msg['addr'] != worker.Addr:
                        print(msg['addr'], worker.Addr)
                    
                    elif command == 'kill':
                        return
                else:
                    self.semaphore.acquire()
                    self.scheduler.remove_worker(worker.Idle)
                    self.semaphore.release()

            time.sleep(2)        

    def msg_thread(self):
        while True: #listen messages forever
            command, msg = self.socket_msg.recv_serialized(msg_deserialize)
            
            if command == 'HELLO':
                #TODO: Make another register method for workers
                worker = Worker(msg['idle'], msg['addr'])
                self.send_code(worker)
                self.semaphore.acquire()
                self.scheduler.register_worker(worker, msg['idle'])
                self.send_scheduler()
                self.semaphore.release()

            elif command == 'DONE':
                task = msg['task']
                resp = msg['response']
                self.semaphore.acquire()
                self.scheduler.submit_task(task, resp)
                self.send_scheduler()
                self.semaphore.release()

            elif command == 'BACKUP':
                self.semaphore.acquire()
                sock = zmq.Context().socket(zmq.PUSH)
                sock.connect(zmq_addr(msg['addr']))
                sock.send_serialized(['SCHEDULER', { 'scheduler': self.scheduler, 'backups': self.backups }], msg_serialize)
                sock.close()
                self.backups.append(msg['addr'])
                self.semaphore.release()

            elif command == 'CHECK':
                sender = zmq.Context().socket(zmq.PUSH)
                sender.connect(zmq_addr(msg['port']))
                sender.send_serialized([None], msg_serialize)

            elif command == 'kill':
                break

            else:
                # report error
                print(command)

    def send_code(self, new_worker):
        sock = self.zmq_context.socket(zmq.PUSH)
        sock.connect(zmq_addr(new_worker.Addr))
        sock.send_serialized(['CODE', { 'mapper' : self.scheduler.config.mapper, 'reducer' : self.scheduler.config.reducer }], msg_serialize)
        print(f'SENDEND to {new_worker.Addr}')
        sock.close()

    def send_task(self, worker, task):
        sock = self.zmq_context.socket(zmq.PUSH)
        sock.connect(zmq_addr(worker.Addr))
        sock.send_serialized(['TASK', {'task': task }], msg_serialize)
        sock.close()

    def send_scheduler(self):
        for backup in self.backups:
            sock = zmq.Context().socket(zmq.PUSH)
            sock.connect(zmq_addr(backup))
            sock.send_serialized(['SCHEDULER', { 'scheduler': self.scheduler }], msg_serialize)
            sock.close()