from .utils import zmq_addr, msg_deserialize, msg_serialize, get_host_ip
from framework.master import MasterNode
from threading import Thread, Semaphore
import zmq
import time

class BackupNode(object):
    """ msg -> 8084 """
    def __init__(self):
        self.host = get_host_ip(lh=True)
        self.addr = zmq_addr(8084, host=self.host)

        # predefined master directions
        self.master_msg = zmq_addr(8081)
        self.master_pong = zmq_addr(8080)

        self.zmq_context = zmq.Context()
        self.tracker_backup = None
        self.backups = None

        self.vote = True

        self.semaphore = Semaphore()

        self.socket = self.zmq_context.socket(zmq.PULL)
        self.socket.bind(self.addr)

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
                self.tracker_backup = msg['scheduler']
            
            elif command == 'PING':
                temp_ctx = zmq.Context()
                sock = temp_ctx.socket(zmq.PUSH)
                sock.connect(self.master_pong)
                sock.send_serialized(['PONG', {'addr': self.addr}], msg_serialize)
                sock.close()

            elif command == 'UPDATE':
                print('UPDATING')
                mss = msg['missing']
                self.semaphore.acquire()
                try:
                    self.backups.remove(mss)
                except:
                    pass
                print('After:', self.backups)
                self.semaphore.release()

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
                self.semaphore.acquire()
                self.tracker_backup = msg['scheduler']
                self.backups = msg['backups']
                print(self.backups, msg['backups'])
                self.semaphore.release()
                break
            else:
                print(command)    

    def ping_to_master(self):
        c = zmq.Context()
        s = c.socket(zmq.PULL)
        port = s.bind_to_random_port(f'tcp://{self.host}') #at moment

        sender = c.socket(zmq.PUSH)
        sender.connect(self.master_msg)
        sender.send_serialized(['CHECK', { 'addr': zmq_addr(port, host=self.host) }], msg_serialize)
        
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
            sender.connect(b)
            sender.send_serialized(['VOTE', None], msg_serialize)

        time.sleep(2)

        if self.vote:
            s = self.zmq_context.socket(zmq.PUSH)
            s.connect(self.addr)
            s.send_serialized(['kill', None], msg_serialize)

            new_master = MasterNode()
            if self.tracker_backup:
                new_master.tracker = self.tracker_backup
                new_master.backups = self.backups
            return new_master
        else:
            #reset votes
            self.vote = True