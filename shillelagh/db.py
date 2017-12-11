from shillelagh.results import get_results


apilevel = '2.0'
paramstyle = 'qmark'

# Threads may share the module and connections.
threadsafety = 2


def connect(host, port=8082):
    return Connection(host, port)


class Connection:

    def __init__(self, host, port):
        self.host = host
        self.port = port

    def cursor(self):
        return Cursor(self.host, self.port)


class Cursor:

    def __init__(self, host, port):
        self.host = host
        self.port = port

        self.results = []

    def execute(self, operation, parameters=None):
        self.results = get_results(apply_parameters(operation, parameters))
        return self

    def fetchall(self):
        return self.results


def apply_parameters(operation, parameters):
    # XXX
    return operation
