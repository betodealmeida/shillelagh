from shillelagh.grammar import Sqlite3Grammar


class ResultsBuilder:

    def __init__(self, parse_tree):
        self.parse_tree = parse_tree

        # The default source has a single row without columns. This allows for
        # building queries without FROM like `SELECT 1`, eg.
        self.source = iter([{}])

    def get_results(self):
        return Sqlite3Grammar.repr_parse_tree(self.parse_tree)


def get_results(query):
    parse_tree = Sqlite3Grammar.parse(query.strip())
    builder = ResultsBuilder(parse_tree)
    return builder.get_results()
