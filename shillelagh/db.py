from shillelagh.grammar import grammar, SQLVisitor


apilevel = '2.0'
threadsafety = 3
paramstyle = 'qmark'


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

        # the default source has a single row with no columns, so we can do
        # things like SELECT 1 from it
        self.source = iter([{}])

        self.results = []

    def execute(self, query):
        tree = grammar.parse(query.strip())
        visitor = SQLVisitor()
        visitor.visit(tree)
        self.results = visitor.results
        return self

    def fetchall(self):
        return self.results
