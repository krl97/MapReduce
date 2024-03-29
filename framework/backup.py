from .utils import zmq_addr, msg_deserialize, msg_serialize, get_host_ip, do_broadcast
from framework.master import MasterNode
from threading import Thread, Semaphore
import zmq
import time

class BackupNode(object):
    """ msg -> 8084 """
    def __init__(self):
        self.host = get_host_ip()
        self.addr = zmq_addr(8084, host=self.host)

        #get master address using broadcast
        master_ip = do_broadcast(self.host, 6666)
        
        if not master_ip:
            raise Exception('Master not found in the network')

        self.master_pong = zmq_addr(8080, host=master_ip)
        self.master_msg = zmq_addr(8081, host=master_ip)

        self.zmq_context = zmq.Context()
        self.tracker_backup = None
        self.backups = None

        self.vote = True

        self.semaphore = Semaphore()

        self.socket = self.zmq_context.socket(zmq.PULL)
        self.socket.bind(self.addr)

    def __call__(self):
        print('BACKUP NODE')
        
        thread = Thread(target=self.thread_recv, name='recv')
        thread.start()

        self.log_in_master()

        while True:
            print('STARTING BACKUP')
            while self.ping_to_master():
                pass

            # here start leader selection (at moment just start a new master)    
            print('STARTING POLL WITH:', self.backups)
            master = self.select_master()
            print('DECIDE NEW MASTER')
            if master:
                master()

    def thread_recv(self):
        while True:
            command, msg = self.socket.recv_serialized(msg_deserialize)
            
            if command == 'SCHEDULER':
                self.tracker_backup = msg['scheduler']
            
            elif command == 'PING':
                temp_ctx = zmq.Context()
                sock = temp_ctx.socket(zmq.PUSH)
                sock.connect(self.master_pong)
                sock.send_serialized(['PONG', {'addr': self.addr}], msg_serialize)
                sock.close()

            elif command == 'NEW_MASTER':
                host = msg['host']
                print('NEW MASTER:', host)
                self.semaphore.acquire()
                self.master_msg = zmq_addr(8081, host=host)
                self.master_pong = zmq_addr(8080, host=host)
                self.semaphore.release()

            elif command == 'UPDATE':
                mss = msg['missing']
                self.semaphore.acquire()
                try:
                    self.backups.remove(mss)
                except:
                    pass
                self.semaphore.release()

            elif command == 'VOTE':
                self.vote = False

            elif command == 'kill':
                break

    # predefined
    def log_in_master(self):
        """ Log in the current master and retry waiting for scheduler backup """
        while True:
            sock = self.zmq_context.socket(zmq.PULL)
            port = sock.bind_to_random_port(f'tcp://{self.host}')

            s = self.zmq_context.socket(zmq.PUSH)
            self.semaphore.acquire()
            s.connect(self.master_msg)
            self.semaphore.release()
            
            s.send_serialized(['BACKUP', {'addr': self.addr, 'reply': zmq_addr(port, host=self.host)}], msg_serialize)

            command, msg = sock.recv_serialized(msg_deserialize)
                
            if command == 'SCHEDULER':
                print('LOGGING IN MASTER')
                self.semaphore.acquire()
                self.tracker_backup = msg['scheduler']
                self.backups = msg['backups']
                self.semaphore.release()
                break
            else:
                print('UNKNOWN COMMAND:', command)


    def ping_to_master(self):
        s = self.zmq_context.socket(zmq.PULL)
        port = s.bind_to_random_port(f'tcp://{self.host}') #at moment

        sender = self.zmq_context.socket(zmq.PUSH)
        self.semaphore.acquire()
        sender.connect(self.master_msg)
        self.semaphore.release()
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
            new_master = MasterNode()
            if self.tracker_backup:
                new_master.tracker = self.tracker_backup
                new_master.backups = self.backups
                new_master.send_identity()

            s = self.zmq_context.socket(zmq.PUSH)
            s.connect(self.addr)
            s.send_serialized(['kill', None], msg_serialize)
            
            return new_master
        else:
            #reset votes
            self.vote = True
            time.sleep(2)