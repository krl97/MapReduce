from uuid import uuid1
from .utils import chunks

#Task State

PENDING = 0
INPROGRESS = 1
COMPLETED = 2

class Scheduler(object):
    def __init__(self, input_file, size):
        M, chs = chunks(input_file, size)
        
        #coordinate m map task and r reduce task
        self.M = M
        self.R = 4 # predefined

        self.workers = { } # Worker -> Task(str)

        self.tasks = { str(i): JTask(str(i), 'map', {'chunk': chunk, 'chunk_idx': i }) for i, chunk in chs } 
        self.tasks_state = { t: PENDING  for t in list(self.tasks.keys()) }
        self.tasks_pending = [t for t in list(self.tasks.keys())]

        self.ikeys = set()

    def register_worker(self, worker, idle):
        """ Register a new worker in the scheduler to receive task,
        register(worker, 0) delete the worker from the scheduler 
        """
        if not idle:
            self.remove_worker(worker.idle)
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
                ntask = self.tasks_pending.pop(0)
                self.workers[worker] = ntask
                self.tasks_state[ntask] = INPROGRESS
                return worker, self.tasks[ntask]
            except:
                return None

    @property
    def tasks_done(self):
        """ Returns True if all workers are without map tasks and 
        any task is pending, if all tasks are done the reduce operation
        can start in workers nodes """
        return all([ not status for _, status in self.workers.items()]) and not len(self.tasks_pending)

    def _get_worker(self):
        for worker, status in self.workers.items():
            if not status:
                return worker

    def _free_worker(self, task_id):
        for worker, status in self.workers.items():
            if status == task_id:
                self.workers[worker] = None

    def submit_task(self, task_id, msg):
        """ Submit a message to the scheduler from a socket to be processed """
        try:
            state = self.tasks_state[task_id]
            if state == COMPLETED:
                return
            func = f'{self.tasks[task_id].Type}_task'
            Scheduler.__dict__[func](self, msg)
            self.tasks_state[task_id] = COMPLETED
            self._free_worker(task_id)
        except:
            pass

    def map_task(self, msg):
        #receive the intermediate keys
        self.ikeys.update(msg['ikeys'])

    def reduce_task(self, msg):
        pass

    def remove_worker(self, idle):
        task = self.workers.pop(idle)
        if task:
            self.tasks_state[task] = PENDING
            self.pendings.append(task)

class JTask(object):
    """ Represents a Job Task created for the Scheduler """

    def __init__(self, task_id, type, body):
        self.task_id = task_id
        self.type = type
        self.body = body

    @property
    def Id(self):
        return self.task_id

    @property
    def Type(self):
        return self.type

    @property
    def Body(self):
        return self.body

    def __eq__(self, other):
        if isinstance(other, str):
            return self.task_id == other
        return self.task_id == other.task_id

    def __hash__(self):
        return self.task_id.__hash__()

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