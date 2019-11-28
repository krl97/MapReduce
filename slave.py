import sys
from framework import WorkerNode 

if __name__ == "__main__":
    addr = sys.argv[1]
    worker = WorkerNode(addr)
    worker()