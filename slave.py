import sys
from framework import WorkerNode 

if __name__ == "__main__":
    addr = sys.argv[1]
    idle = sys.argv[2]
    worker = WorkerNode(addr, int(idle))
    worker()