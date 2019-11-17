def zmq_addr(port, transport=None, host=None):
    if host is None:
        host = '127.0.0.1'

    if transport is None:
        transport = 'tcp'

    assert transport in transports
    assert 1000 < port < 10000

    return f'{transport}://{host}:{port}'