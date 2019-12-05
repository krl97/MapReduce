from framework.utils import msg_serialize, msg_deserialize, get_host_ip, do_broadcast, zmq_addr
import zmq

def mapreduce(config):
    """ Submit a new job in the jobstracker using a config class """
    # connect to master and submit a config class to
    # log a new job in the scheduler
    host = get_host_ip()
    #try get master_ip from network
    master_ip = do_broadcast(host, 6666)
    
    if not master_ip:
        raise Exception('Master not founded in cluster')

    print('SENDING JOB')

    c = zmq.Context()
    s_send = c.socket(zmq.PUSH)
    s_send.connect(zmq_addr(8081, host=master_ip))

    # here send the direction to receive the response
    sock = c.socket(zmq.PULL)
    port = sock.bind_to_random_port(f'tcp://{host}')

    config.r_addr = zmq_addr(port, host=host)
    s_send.send_serialized(['JOB', { 'config': config }], msg_serialize)
    
    state, msg = sock.recv_serialized(msg_deserialize)

    print('JOB STATE:', state)

    s_send.close() 