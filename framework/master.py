from .utils import zmq_addr, msg_deserialize, msg_serialize, get_host_ip, waiting_to_broadcast
from .scheduler import Scheduler, Worker, JTask, JobsTracker
from threading import Thread, Semaphore
import dill
import os
import time
import zmq

class MasterNode(object):
    """ msg -> 8081 | ping -> 8080 """
    def __init__(self):
        self.host = get_host_ip()
        self.addr_msg = zmq_addr(8081, host=self.host)
        self.addr_pong = zmq_addr(8080, host=self.host)
        
        self.zmq_context = zmq.Context() 

        self.socket_pong = self.zmq_context.socket(zmq.PULL)
        self.socket_pong.bind(self.addr_pong)
        self.socket_msg = self.zmq_context.socket(zmq.PULL)
        self.socket_msg.bind(self.addr_msg)
        
        self.semaphore = Semaphore()
        self.backups = [ ]

        self.tracker = JobsTracker() 
        
        self.results = [ ]

    def __call__(self):
        _msg_thread = Thread(target=self.msg_thread, name="msg_thread")
        _ping_thread = Thread(target=self.ping_thread, name="ping_thread")
        _broadcast_thread = Thread(target=self.broadcast_thread, name="broadcast_thread")
        _broadcast_thread.start()
        _msg_thread.start()
        _ping_thread.start() 

        print('NODE MASTER')

        while True:
            print('-- STARTING MASTER --')

            # work loop
            while True:
                self.semaphore.acquire()

                # check for states
                try:
                    nxt_state = self.tracker.scheduler.next_state()
                    if nxt_state:   
                        raddr = nxt_state()
                        if raddr:
                            self.send_to_client(raddr, 'JOB ENDED', None)
                    else:
                        next_task = self.tracker.scheduler.next_task()

                        if next_task:
                            worker, task = next_task
                            self.send_task(worker, task)
                except:
                    pass

                if self.tracker.next_job():
                    print('--- STARTING THE NEXT JOB ---')

                self.semaphore.release()                    

        self.shutdown_cluster()
        
    def shutdown_cluster(self):
        sock = self.zmq_context.socket(zmq.PUSH)
        sock.connect(self.addr_msg)
        sock.send_serialized(['kill', None], msg_serialize)

        for w in self.tracker.scheduler.workers.keys():
            sock = self.zmq_context.socket(zmq.PUSH)
            sock.connect(w.Addr)
            sock.send_serialized(['SHUTDOWN', None], msg_serialize)


    def broadcast_thread(self):
        waiting_to_broadcast(self.host, 6666)

    def ping_thread(self):
        c = zmq.Context()
        while True:
            self.semaphore.acquire()
            workers = [w for w in list(self.tracker.scheduler.workers.keys())]
            self.semaphore.release()

            for worker in workers:
                sock = c.socket(zmq.PUSH)
                sock.connect(worker.pong_addr)
                sock.send_serialized(['PING', None], msg_serialize, zmq.NOBLOCK)
                
                poller = zmq.Poller()
                poller.register(self.socket_pong, zmq.POLLIN)
            
                sck = dict(poller.poll(2000))
                if sck:
                    command, msg = self.socket_pong.recv_serialized(msg_deserialize, zmq.NOBLOCK)
                    
                    if command == 'PONG' and msg['addr'] != worker.Addr:
                        # print('VIEW:', msg['addr'], worker.Addr)
                        pass
                    
                    elif command == 'kill':
                        return
                else:
                    self.semaphore.acquire()
                    self.tracker.scheduler.remove_worker(worker.Idle)
                    self.semaphore.release()

            self.semaphore.acquire()
            backups = list(self.backups)
            self.semaphore.release()

            for b in backups:
                sock = c.socket(zmq.PUSH)
                sock.connect(b)
                sock.send_serialized(['PING', None], msg_serialize, zmq.NOBLOCK)
                
                poller = zmq.Poller()
                poller.register(self.socket_pong, zmq.POLLIN)
            
                sck = dict(poller.poll(2000))
                if sck:
                    command, msg = self.socket_pong.recv_serialized(msg_deserialize, zmq.NOBLOCK)
                    
                    if command == 'PONG' and msg['addr'] != b:
                        #print('VIEW:', msg['addr'], b)
                        pass

                    elif command == 'kill':
                        return
                else:
                    self.semaphore.acquire()
                    self.backups.remove(b)
                    self.update_backups(b)
                    self.semaphore.release()


            time.sleep(3)        

    def msg_thread(self):
        while True:
            command, msg = self.socket_msg.recv_serialized(msg_deserialize)
            
            if command == 'HELLO':
                worker = Worker(msg['idle'], msg['addr'], msg['paddr'])
                self.semaphore.acquire()
                self.send_reply(worker)
                self.tracker.scheduler.register_worker(worker, msg['idle'])
                self.send_scheduler()
                self.semaphore.release()

            elif command == 'JOB':
                self.semaphore.acquire()
                config = msg['config']
                self.tracker.submit_job(config)
                self.send_scheduler()
                self.semaphore.release()

            elif command == 'DONE':
                task = msg['task']
                resp = msg['response']
                self.semaphore.acquire()
                self.tracker.scheduler.submit_task(task, resp)
                self.send_scheduler()
                self.semaphore.release()

            elif command == 'BACKUP':
                self.semaphore.acquire()
                sock = self.zmq_context.socket(zmq.PUSH)
                sock.connect(msg['reply'])
                print('NEW BACKUP IN CLUSTER:', msg['addr'])
                sock.send_serialized(['SCHEDULER', { 'scheduler': self.tracker, 'backups': self.backups }], msg_serialize)
                self.backups.append(msg['addr'])
                self.semaphore.release()

            elif command == 'CHECK':
                sender = self.zmq_context.socket(zmq.PUSH)
                sender.connect(msg['addr'])
                sender.send_serialized([None], msg_serialize)

            elif command == 'kill':
                break

            else:
                # report error
                print('UNKNOWN COMMAND:', command)

    def send_reply(self, worker):
        sock = self.zmq_context.socket(zmq.PUSH)
        sock.connect(worker.Addr)
        sock.send_serialized(['REPLY', None], msg_serialize)
        print(f'REPLY to {worker.Addr}')
        sock.close()

    def send_task(self, worker, task):
        sock = self.zmq_context.socket(zmq.PUSH)
        sock.connect(worker.Addr)
        sock.send_serialized(['TASK', {'task': task }], msg_serialize)
        sock.close()

    def send_scheduler(self):
        for backup in self.backups:
            sock = self.zmq_context.socket(zmq.PUSH)
            sock.connect(backup)
            sock.send_serialized(['SCHEDULER', { 'scheduler': self.tracker }], msg_serialize)
            sock.close()

    def update_backups(self, missing):
        for b in self.backups:
            sock = self.zmq_context.socket(zmq.PUSH)
            sock.connect(b)
            sock.send_serialized(['UPDATE', { 'missing' : missing }], msg_serialize)
            sock.close()

    def send_identity(self):
        nodes = [w.Addr for w in list(self.tracker.scheduler.workers.keys())]
        nodes = list(self.backups) + nodes
        for node in nodes:
            s = self.zmq_context.socket(zmq.PUSH)
            s.connect(node)
            s.send_serialized(['NEW_MASTER', {'host': self.host}], msg_serialize)

        time.sleep(1) 

    def send_to_client(self, addr, status, msg):
        s = self.zmq_context.socket(zmq.PUSH)
        s.connect(addr)
        s.send_serialized([status, msg], msg_serialize)