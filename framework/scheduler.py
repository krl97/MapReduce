from uuid import uuid1
from .utils import chunks, str_hash
from os.path import isdir, relpath

#Task State

PENDING = 0
INPROGRESS = 1
COMPLETED = 2

START = 'start'
MAP = 'map'
REDUCE = 'reduce'
FINISH = 'finish'

# TODO: Use the scheduler for track > 1 jobs and workers with code
class Scheduler(object):
    def __init__(self):
        # use this values in the scheduler states and for multiple jobs
        self.config = None
        self.input_file = None
        self.size = None
        self.output_folder = None

        self.workers = { } # Worker -> Task(str)
        
        self.tasks = { }
        self.tasks_state = { }
        self.tasks_pending = [ ]
        
        self.ikeys = { }
        
        self.states = None
        self.current_state = None

        self.mappers = set()

    def submit_job(self, config):
        self._reset()

        self.config = config
        self.input_file = config.input
        self.size = config.chunk_size
        self.output_folder = config.output_folder

        #here set scheduler states for this job
        self.states = [(MAP, self.init_map), 
                       (REDUCE, self.init_reduce)]
        self.current_state = START

    def next_state(self):
        """ Returns a function to init the next state, None if the current state
        still in progress, next_state raise Exception if scheduler workflow ends """
        if self.current_state == FINISH:
            print('HERE')
            raise Exception('Scheduler in finish state, submit a new job')
        
        if self.move_next:
            state_id, state = self.states.pop(0)
            self.current_state = state_id
            return state

    @property
    def move_next(self):
        """ Returns True if the current state are finished and the function
        next_state return the next state in the execution """
        if self.current_state == START:
            return self.with_job

        return all([ not status for _, status in self.workers.items()]) and not len(self.tasks_pending)

    @property
    def with_job(self):
        return self.config != None

    def register_worker(self, worker, idle):
        """ Register a new worker in the scheduler to receive task,
        register(worker, 0) delete the worker from the scheduler 
        """
        if not idle:
            self.remove_worker(worker.idle)
        else:
            self.workers.setdefault(worker, None)
    
    def remove_worker(self, idle):
        task = self.workers.pop(idle)
        print(self.workers)
        print(task, idle)
        if task:
            self.tasks_state[task] = PENDING
            self.tasks_pending.append(task) 

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
        state = self.tasks_state[task_id]
        if state == COMPLETED:
            return
        func = f'{self.tasks[task_id].Type}_task'
        Scheduler.__dict__[func](self, msg)
        self.tasks_state[task_id] = COMPLETED
        self._free_worker(task_id)
    def map_task(self, msg):
        self.mappers.add(msg['addr'])
        res = msg['ikeys']
        for ikey, value in res:
            try:
                self.ikeys[ikey].append(value)
            except:
                self.ikeys[ikey] = [ value ]
        
    def reduce_task(self, msg):
        addr = msg['addr']
        res = msg['output']
        assert isdir(self.output_folder), 'The directory don\'t exist'
        name = f'{relpath(self.output_folder)}/{addr}'
        f = open(name, 'a')
        f.writelines('\n'.join(f'{ikey}-{val}' for ikey, val in res))
        f.write('\n')

    def init_map(self):
        M, chs = chunks(self.input_file, self.size)
        
        for i, chunk in chs:
            id = str(i)
            self.tasks[id] = JTask(str(id), 'map', {'mapper': self.config.mapper ,'chunk': chunk, 'chunk_idx': i }) 
            self.tasks_state[id] = PENDING
            self.tasks_pending.append(id)

    def init_reduce(self):
        r = len(self.mappers)
        partitions = [ [ ] for _ in range(r) ]  

        for ikey in list(self.ikeys.keys()):
            partitions[str_hash(ikey) % r].append((ikey, self.ikeys[ikey]))

        for i in range(r):
            id = uuid1().hex
            self.tasks[id] = JTask(id, 'reduce', {'reducer' : self.config.reducer , 'partition': partitions[i] })
            self.tasks_state[id] = PENDING
            self.tasks_pending.append(id)

    def _reset(self):
        self.config = None
        self.input_file = None
        self.size = None
        self.output_folder = None
        self.tasks = { }
        self.tasks_state = { }
        self.tasks_pending = [ ]
        self.ikeys = { }
        self.mappers = set()
        self.states = None
        self.current_state = None

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
        self.pong_addr = str(int(self.addr) + 1)
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