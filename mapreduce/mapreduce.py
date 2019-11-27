from framework.utils import msg_serialize
import zmq

def mapreduce(config, wait=False):
    # connect to master and submit a config class to
    # log a new job in the scheduler

    c = zmq.Context()
    s_send = c.socket(zmq.PUSH)
    s_send.connect('tcp://127.0.0.1:5000')
    s_send.send_serialized([config], msg_serialize)
    s_send.close()

    # use wait flag to stop user program execution 