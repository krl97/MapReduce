from uuid import uuid1
from .utils import chunks

#Task State

PENDING = 0
INPROGRESS = 1
COMPLETED = 2

class Scheduler(object):
    def __init__(self, input, size):
        chs = chunks(file, size)
        
        #coordinate m map task and r reduce task
        self.M = len(chs)
        self.R = 4 # predefined

        self.workers = { } # Worker -> Task(str)

        self.tasks = { JTask(i, 'map', {'chunk' : chunk, 'chunk_idx' : i }) : PENDING for i, chunk in chs } 
        self.pendings = [t.id for t in list(self.tasks.keys())]

        self.ikeys = [ ]

    def register_worker(self, worker, idle):
        """ Register a new worker in the scheduler to receive task,
        register(worker, 0) delete the worker from the scheduler """

        if not idle:
            self.remove_worker(idle)
        else:
            self.workers.setdefault(worker, None)
    
    def is_registered(self, worker):
        """ Returns True if the worker is registered """

        return not self.workers.keys().isdisjoint([ worker ])

    def next_task(self):
        """ Assign the next task to a availabe worker, returns a tuple 
        worker, task """

        worker = self._get_worker()
        
        if worker:
            try:
                ntask = self.pendings.pop(0)
                self.workers[worker] = ntask
                self.tasks[ntask] = INPROGRESS
                return worker, self.tasks[ntask]
            except:
                return None

    def _get_worker(self):
        for worker, status in self.workers.items():
            if not status:
                return worker

    def submit_task(self, task, msg):
        """ Submit a message to the scheduler from a socket to be processed """

        try:
            state = self.tasks[task]
            if state == COMPLETED:
                return
            func = f'{task.type}_task'
            self.__dict__[func](msg)
            self.tasks[task] = COMPLETED
        except:
            pass

    def map_task(self, msg):
        #receive the intermediate keys
        self.ikeys += msg['ikeys']

    def reduce_task(self, msg):
        pass

    def _remove_worker(self, idle):
        task = self.workers.pop(idle)
        if task:
            self.tasks[task] = PENDING
            self.pendings.append(task)

class JTask(object):
    """ Represents a Job Task created for the Scheduler """
    
    def __init__(self, task_id, type, body):
        self.task_id = task_id
        self.type = type
        self.body = body

    @property
    def id(self):
        return task_id

    @property
    def type(self):
        return self.type

    @property
    def body(self):
        return self.body

    def __eq__(self, other):
        if isinstance(other, str):
            return self.task_id == other
        return self.task_id == other.task_id

    def __repr__(self):
        return self.task_id

    def __str__(self):
        return self.__repr__()

    def __hash__(self):
        return self.task_id.__hash__()

class Worker(object):
    """ Represents a Worker for the Scheduler """

    def __init__(self, idle, addr):
        self.addr = addr
        self.idle = idle

    @property
    def addr(self):
        return self.addr

    @property
    def idle(self):
        return self.idle

    def __eq__(self, other):
        if isinstance(other, str):
            return self.idle == other
        return self.idle == other.idle

    def __hash__(self):
        return self.idle.__hash__()