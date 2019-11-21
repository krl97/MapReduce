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

        self.tasks = { JTask(i, 'map', {
            'chunk' : chunk,
            'chunk_idx' : i
        }) , PENDING for i, chunk in chs } 

    def register_worker(self, worker, idle):
        """ Register a new worker in the scheduler to receive task.
        register(worker, 0) delete the worker from the scheduler """

        if not idle:
            self.remove_worker(idle)
        else:
            try:
                self.workers[idle]
            except:
                self.workers[idle] = None
        

    def submit_response(self, msg):
        """ Submit a message to the scheduler from a socket to be processed """

        pass

    def _remove_worker(self, idle):
        task = self.workers.pop(idle)
        if task:
            self.tasks[task] = PENDING
            # enqueue task again (some checks)
        
        


class Worker(object):
    """ Represents a Worker for the Scheduler """

    def __init__(self, idle, addr):
        self.addr = addr
        self.idle = idle

    @property
    def Addr(self):
        return self.addr

    @property
    def Idle(self):
        return self.idle

    def __eq__(self, other):
        if isinstance(other, str):
            return self.idle == other
        return self.idle == other.idle

    def __hash__(self):
        return self.idle.__hash__()

class JTask(object):
    """ Represents a Job Task created for the Scheduler """
    
    def __init__(self, task_id, task_type, body):
        self.task_id = task_id
        self.task_type = task_type
        self.body = body

    @property
    def Task_Id(self):
        return task_id

    @property
    def Task_Type(self):
        return self.task_type

    @property
    def Body(self):
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
    