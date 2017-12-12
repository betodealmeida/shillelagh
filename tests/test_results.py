# -*- coding: utf-8 -*-

from .context import shillelagh

from functools import wraps
import unittest

import nose


@nose.tools.nottest
def docstring(test):
    query = test.__doc__
    connection = shillelagh.db.connect('localhost')
    cursor = connection.cursor()
    expected = cursor.execute(query).fetchall()

    @wraps(test)
    def test_wrapper(self):
        result = test(self)
        self.assertEquals(result, expected)

    return test_wrapper


class ResultsTestSuite(unittest.TestCase):

    @docstring
    def test_numeric_literal(self):
        """
        SELECT 1;
        """
        return [(1,)]

    @docstring
    def test_string_literal(self):
        """
        SELECT 'string', "identifier" FROM a;
        """
        return [('a',)]


if __name__ == '__main__':
    unittest.main()
