import pkg_resources

from parsimonious.grammar import Grammar, NodeVisitor


peg = pkg_resources.resource_string('shillelagh', 'grammar/sqlite.peg')
grammar = Grammar(peg.decode())


class SQLVisitor(NodeVisitor):

    def __init__(self):
        self.source = iter([{}])
        self.result_columns = []

    @property
    def results(self):
        return [
            tuple(
                result_column(row)
                for result_column in self.result_columns
            )
            for row in self.source
        ]

    def visit_result_column(self, node, visited_children):
        return self.visit(node.children[1])

    def visit_expr(self, node, visited_children):
        return self.visit(node.children[1])

    def visit_value(self, node, visited_children):
        return self.visit(node.children[0])

    def visit_literal_value(self, node, visited_children):
        return self.visit(node.children[0])

    def visit_numeric_literal(self, node, visited_children):
        self.result_columns.append(lambda row: int_or_float(node.text))

    def visit_string_literal(self, node, visited_children):
        return self.visit(node.children[1])

    def generic_visit(self, node, visited_children):
        return node.text or visited_children


def int_or_float(s):
    try:
        return int(s)
    except TypeError:
        return float(s)
