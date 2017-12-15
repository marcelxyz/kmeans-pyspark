import unittest
import helpers


class HelpersTest(unittest.TestCase):
    def test_datetime_to_timestamp(self):
        timestamp = helpers.datetime_to_timestamp('2017-12-15T14:01:10.123')
        self.assertEqual(timestamp, 1513346470)

    def test_is_valid_tuple(self):
        self.assertFalse(helpers.is_valid_tuple((0, None), 2))
        self.assertFalse(helpers.is_valid_tuple((0, 1), 3))
        self.assertTrue(helpers.is_valid_tuple((4, 5), 2))
