import sys
from framework import WorkerNode 

if __name__ == "__main__":
    idle = sys.argv[1]
    worker = WorkerNode(idle)
    worker()