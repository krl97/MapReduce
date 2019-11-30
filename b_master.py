import sys
from framework import BackupNode

if __name__ == "__main__":
    port = sys.argv[1]
    backup = BackupNode(port)
    backup()
    