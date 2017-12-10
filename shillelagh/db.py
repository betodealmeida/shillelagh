from .grammar import grammar

from parsimonious.nodes import NodeVisitor


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

        self.results = []

    def execute(self, query):
        tree = grammar.parse(query.strip())
        self.results = SQLVisitor().visit(tree).results()
        return self

    def fetchall(self):
        return self.results


class SQLVisitor(NodeVisitor):

    def results(self):
        return []

    def visit_whitespace(self, node, children):
        return str(node.text)
