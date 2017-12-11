import pkg_resources

from parsimonious.grammar import Grammar, NodeVisitor


peg = pkg_resources.resource_string('shillelagh', 'grammar/sqlite.peg')
grammar = Grammar(peg.decode())


class SQLVisitor(NodeVisitor):

    def __init__(self):
        self.results = []

    def visit_whitespace(self, node, visited_children):
        return str(node.text)

    def visit_select_core(self, node, visited_children):
        self.results.append((self.visit(node.children[1]),))

    def visit_result_column(self, node, visited_children):
        return self.visit(node.children[1])

    def visit_expr(self, node, visited_children):
        return self.visit(node.children[0])

    def visit_value(self, node, visited_children):
        return self.visit(node.children[0])

    def visit_literal_value(self, node, visited_children):
        return self.visit(node.children[0])

    def visit_numeric_literal(self, node, visited_children):
        return int_or_float(node.text)

    def visit_string_literal(self, node, visited_children):
        return self.visit(node.children[1])

    def generic_visit(self, node, visited_children):
        return node.text or visited_children


def int_or_float(s):
    try:
        return int(s)
    except TypeError:
        return float(s)
