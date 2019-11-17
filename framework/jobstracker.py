from utils import zmq_addr
from threading import Thread, Semaphore
import zmq

class MasterNode(object):
    def __init__(self, workers, config):
        self.addr_task = zmq_addr(8080)
        self.addr_msg = zmq_addr(8081)
        
        self.zmq_context = zmq.Context() 

        self.socket_task = self.zmq_context.socket(zmq.PAIR)
        self.socket_task.bind(self.addr_task)
        self.socket_msg = self.zmq_context.socket(zmq.PAIR)
        self.socket_msg.bind(self.addr_msg)

        self.config = config
        self.semaphore = Semaphore()

        self.workers = { worker : 'non-task' for worker in workers }
        self.map_routes = []
        self.chunk_size = config.chunk_size # number of lines
        self.current_chunk = (0, self.chunk_size)
        self.chunks_state = ['pending'] * M
        self.M = MasterNode.map_splitter(self.config.input, self.chunk_size)

    def __call__(self):
        #here start thread for incoming message from workers
        msg_thr = Thread(target=self.msg_thread, name="msg_thread")
        msg_thr.start()

        M = self.M  #get number of map task to send 

        # send all map task
        while M and self.map_task(): 
            M -= 1

        # wait form map workers
        while True:
            self.semaphore.acquire()
            if all([state == 'completed' for state in self.chunks_state]):
                self.semaphore.release()
                break
            self.semaphore.release()

        print(self.map_routes)

    @staticmethod
    def map_splitter(file, size):
        f = open(file, 'r')
        lines = len(f.readlines())
        return int(lines / size) + 1

    def get_worker(self):
        self.semaphore.acquire()
        res = None
        for worker, state in self.workers.items():
            if state in ['non-task', 'completed']:
                res = worker
                break
        self.semaphore.release()

    def msg_thread(self):
        while True: #listen messages forever
            msg = self.socket_msg.recv_json()

            #parse message
            if msg['status'] == 'END':
                worker = msg['idle']
                task = msg['task']
                chunk_idx = msg['chunk']

                if task == 'map-task':
                    self.semaphore.acquire()
                    self.workers[worker] = 'completed'
                    self.chunks_state[chunk_idx] = 'completed'
                    self.map_routes.append(msg['route'])
                    self.semaphore.release()
                    print(f'Worker: {worker} end map-task')
                else:
                    #other task
                    print(f'Worker talking about: {task}')

            else:
                # report error
                print(msg['status'])

    def send_task(self, worker, data):
        socket_worker = self.zmq_context.socket(zmq.PAIR)
        socket_worker.connect(zmq_addr(worker))
        socket_worker.send_json(data)
        
        #TODO: add timeout for waiting
        reply = self.socket_task.recv_json()  
        return reply   

    def map_task(self):
        """  Assign a map task """
        worker = self.get_worker()

        if worker:
            # build the data
            data = {'task' : 'map',
                    'class' : config.Mapper, #serialized with dill ( this maybe come with a parsing function and local group function)
                    'file' : config.input,
                    'chunk' : self.current_chunk 
                }

            reply = self.send(worker, data)

            if reply['status'] == 'RECV':
                self.semaphore.acquire()
                self.chunks_state[self.current_chunk[0] / self.chunk_size] = 'in-progress' #set sended chunk
                self.current_chunk = (self.current_chunk[1], self.current_chunk[1] + self.chunk_size
                self.workers[worker] = 'map-task'
                self.semaphore.release()
                return True

        return False

    #TODO: Get values from network 
    def reduce_task(self):
        pass

if __name__ == "__main__":
    class MockConfig(object):
        self.