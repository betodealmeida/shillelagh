import pkg_resources

from parsimonious.grammar import Grammar


peg = pkg_resources.resource_string('shillelagh', 'grammar/sqlite.peg')
grammar = Grammar(peg.decode())
