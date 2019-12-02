from .utils import zmq_addr, msg_deserialize, msg_serialize
from framework.jobstracker import MasterNode
from threading import Thread
import zmq
import time

class BackupNode(object):
    def __init__(self, addr):
        self.addr = addr

        # predefined master directions
        self.master_pong = zmq_addr(8080)
        self.master_msg = zmq_addr(8081)

        self.zmq_context = zmq.Context()
        self.scheduler_backup = None
        self.backups = None

        self.vote = True

        self.socket = self.zmq_context.socket(zmq.PULL)
        self.socket.bind(zmq_addr(addr))

    def __call__(self):
        print('Starter')
        self.log_in_master()
        
        thread = Thread(target=self.thread_recv, name='recv')
        thread.start()

        while True:
            print('START')
            while self.ping_to_master():
                pass

            # here start leader selection (at moment just start a new master)    
            print('WAKE UP', self.backups)
            master = self.select_master()
            print('DECISION')
            if master:
                master()

    def thread_recv(self):
        while True:
            command, msg = self.socket.recv_serialized(msg_deserialize)
            
            if command == 'SCHEDULER':
                print('RECV')
                self.scheduler_backup = msg['scheduler']
                
            elif command == 'VOTE':
                self.vote = False

            elif command == 'kill':
                break

    # predefined
    def log_in_master(self):
        """ Log in the current master and retry waiting for scheduler backup """
        while True:
            s = self.zmq_context.socket(zmq.PUSH)
            s.connect(self.master_msg)
            s.send_serialized(['BACKUP', {'addr': self.addr}], msg_serialize)

            command, msg = self.socket.recv_serialized(msg_deserialize)
            if command == 'SCHEDULER':
                print('First RECV')
                self.scheduler_backup = msg['scheduler']
                self.backups = msg['backups']
                print(self.backups)
                break
            else:
                print(command)    

    def ping_to_master(self):
        c = zmq.Context()
        s = c.socket(zmq.PULL)
        port = s.bind_to_random_port('tcp://127.0.0.1') #at moment

        sender = c.socket(zmq.PUSH)
        sender.connect(self.master_msg)
        sender.send_serialized(['CHECK', { 'port': port }], msg_serialize)
        
        poller = zmq.Poller()
        poller.register(s, zmq.POLLIN)
        
        d = dict(poller.poll(timeout=2000))
        
        if d != {}:
            return True
        else: 
            return False

    def select_master(self):
        for b in self.backups:
            sender = self.zmq_context.socket(zmq.PUSH)
            sender.connect(zmq_addr(b))
            sender.send_serialized(['VOTE', None], msg_serialize)

        time.sleep(2)

        if self.vote:
            s = self.zmq_context.socket(zmq.PUSH)
            s.connect(zmq_addr(self.addr))
            s.send_serialized(['kill', None], msg_serialize)

            new_master = MasterNode()
            if self.scheduler_backup:
                new_master.scheduler = self.scheduler_backup
                new_master.backups = self.backups
            return new_master
        else:
            #reset votes
            self.vote = True