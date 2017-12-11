#!/usr/bin/python -W default
#
# Demo of lrparsing.py - Sqlite3 Data Manipulation Statements (delete, insert,
# select, update).
#
# Copyright (c) 2013,2014,2015,2016,2017 Russell Stuart.
# Licensed under GPLv2, or any later version.
#
from lrparsing import (
    Grammar, Keyword, List, Opt, Prio, Repeat, Ref, THIS, Token, Tokens,
    TokenRegistry)


class Sqlite3Grammar(Grammar):
    #
    # Tokens that are too large to re-type.
    #
    class T(TokenRegistry):
        blob = Token(re="[xX]'(?:[0-9a-z][0-9a-z])*'")
        ident = Token(re='[A-Za-z_][A-Za-z_0-9]*|"[A-Za-z_][A-Za-z_0-9]*"')
        integer = Token(re='[0-9]+')
        float = Token(re='(?:[0-9]+[.][0-9]*|[.][0-9]+)(?:[Ee][-+]?[0-9]+)?')
        param = Token(
            re=(
                '(?:[?](?:[0-9]*|[a-z_][a-z_0-9]*)?|[@:][A-Za-z_][A-Za-z_0-9]*'
                '|[$][A-Za-z_][A-Za-z_0-9]*(?:::([^)]*))*)'
            )
        )
        quoted_field = Token(
            re='"(?:[A-Za-z_][A-Za-z_0-9]*[.][A-Za-z_][A-Za-z_0-9]*")')
        string = Token(re="(?:'[^']')+")
    #
    # Kwd generates a case insensitive keyword.
    #

    def Kwd(k): return Keyword(k, case=False)
    #
    # Toks() generates a list of case insensitive tokens.
    #

    def Toks(t, k=None):
        return Tokens(t, k, case=False)
    #
    # Forward references.
    #
    condition = Ref('condition')
    expr = Ref('expr')
    from_source = Ref('from_source')
    query = Ref("query")
    select_value = Ref("select_value")
    value = Ref("value")
    #
    # Primaries.
    #
    database = T.ident + '.'
    table = Opt(database) + T.ident
    field = Opt(table + '.') + T.ident | T.quoted_field
    number = T.integer | T.float
    call = (
        T.ident +
        '(' + Opt("*" | Opt(Kwd("distinct")) + List(expr, ',', 1)) + ')')
    cast = (
        Kwd("cast") + "(" +
        expr + Kwd("as") + Tokens("", "text real integer blob null") +
        ")")
    atom = (
        call | cast | field | number | select_value | T.blob | T.param |
        Toks("", "null current_date current_time current_timestamp"))
    #
    # Primaries can be built up into numeric and string expressions.
    #
    _value_ops = Prio(
        value << Kwd("collate") << T.ident,
        Toks("+ - ~") >> value,
        value << '||' << value,
        value << Toks("* / %") << value,
        value << Toks("+ -") << value,
        value << Toks("<< >> | &") << value)
    _case1 = (
        Kwd('case') + expr +
        Repeat(Kwd('when') + expr + Kwd('then') + expr, 1) +
        Opt(Kwd('else') + expr) +
        Kwd('end'))
    _case2 = (
        Kwd('case') +
        Repeat(Kwd('when') + condition + Kwd('then') + expr, 1) +
        Opt(Kwd('else') + expr) +
        Kwd('end'))
    value = (
        atom | '(' + value + ')' | _value_ops | _case1 | _case2)
    #
    # Boolean's can be combined using 'and', 'or', and 'not'.
    #
    _string_test_op = Opt(Kwd("not")) + Toks("", "match like glob regexp")
    condition = Prio(
        '(' + THIS + ')' | call,
        value + Toks("< <= >= >") + value,
        (
            expr << Toks("= == != <>", "is") << expr |
            value + Kwd("is") + Kwd("not") + value |
            value + _string_test_op + value + Opt(Kwd("escape") + value) |
            (
                value + Opt(Kwd('not')) + Kwd("in") + '(' +
                List(expr, ',', 1) + ')'
            ) |
            value + Opt(Kwd('not')) + Kwd("in") + select_value |
            value + Opt(Kwd('not')) + Kwd("in") + table |
            (
                value + Opt(Kwd('not')) + Kwd('between') + value + Kwd('and') +
                value
            ) |
            Kwd('exists') + '(' + query + ')'
        ),
        Kwd("not") >> THIS,
        THIS << Kwd("and") << THIS,
        THIS << Kwd("or") << THIS,)
    expr = Prio(value, condition)
    #
    # A select clause.
    #
    alias = Kwd("as") + T.ident
    select = (
        Kwd('select') + Opt(Kwd("all") | Kwd("distinct")) +
        List(expr + Opt(alias) | T.ident + "." + "*" | "*", ',', 1))
    #
    # The from clause is a complex beast.
    #
    _index = (
        Kwd("indexed") + Kwd("by") + T.ident |
        Kwd("not") + Kwd("indexed"))
    single_source = (
        table + Opt(alias) + Opt(_index) |
        "(" + query + ")" + Opt(alias) |
        '(' + from_source + ')')
    _join_kind = (
        Kwd("left") + Opt(Kwd("outer")) |
        Kwd("inner") |
        Kwd("cross"))
    _join_op = ',' | Opt(Kwd('natural')) + Opt(_join_kind) + Kwd("join")
    join_constraint = (
        Kwd("on") + condition |
        Kwd("using") + "(" + List(T.ident, ',', 1) + ")")
    from_source = (
        single_source +
        Repeat(_join_op + single_source + Opt(join_constraint)))
    frm = Kwd('from') + from_source
    #
    # Where is just a boolean condition.  Group by can be followed by
    # having.
    #
    where = Kwd('where') + condition
    group_by = Kwd('group') + Kwd('by') + List(field | T.integer, ",", 1)
    having = Kwd('having') + condition
    #
    # A query is a normal select.  A single select returns a single
    # column.
    #
    query = select + Opt(frm) + Opt(where) + Opt(group_by + Opt(having))
    #
    # Query'es can be compounded.
    #
    order_term = (
        Opt(Kwd("collate") + T.ident) + expr +
        Opt(Toks("", "asc ascending desc descending")))
    order_by = Kwd('order') + Kwd('by') + List(order_term, ",", 1)
    compound_op = (
        Kwd("union") + Opt(Kwd("all")) | Toks("", "except intersect"))
    compound_select = List(query, compound_op, 1) + Opt(order_by)
    single_select = (
        Kwd('select') + Opt(Kwd('distinct') | Kwd("all")) +
        expr + Opt(alias))
    single_query = (
        single_select + Opt(frm) + Opt(where) +
        Opt(group_by + Opt(having)))
    select_value = '(' + List(single_query, compound_op, 1) + ')'
    #
    # Delete can specify an index, apparently.
    #
    delete = (
        Kwd('delete') + Kwd('from') + table + Opt(_index) + Opt(where))
    #
    # Insert can say what to do on failure.
    #
    _fail_op = Kwd("or") + Toks("", "rollback abort replace fail ignore")
    insert_columns = "(" + List(T.ident, ',', 1) + ")"
    values = "(" + List(expr, ',', 1) + ")"
    _insert_data = (
        Kwd("default") + Kwd("values") |
        Opt(insert_columns) + Kwd("values") + List(values, ',', 1) |
        Opt(insert_columns) + query)
    insert = (
        Kwd('insert') + Opt(_fail_op) + Kwd('into') + table + _insert_data)
    #
    # Update can also say what to do on failure.
    #
    update = (
        Kwd('update') + Opt(_fail_op) + table + Opt(_index) +
        Kwd('set') + List(T.ident + '=' + value, ',', 1) +
        Opt(where))
    #
    # Not sure what sqlite3 accepts, but comments of the form:
    #   /* comment */ and
    #   # ... end of line
    # seem reasonable.
    #
    COMMENTS = (
        Token(re='/[*](?:[^*]|[*][^/])*[*]/') |
        Token(re='#[^\r\n]*'))
    statement = (
        Opt(Kwd("explain") + Opt(Kwd("query") + Kwd("plan"))) +
        (delete | insert | compound_select | update))
    START = List(statement, ';', 1, opt=True)
