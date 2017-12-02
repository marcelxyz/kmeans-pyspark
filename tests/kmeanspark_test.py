import unittest
import kmeanspark as k


class KmeansparkTest(unittest.TestCase):
    def test_get_value_for_field(self):
        xml = '<row Id=" 1" Reputation="5555 " />'
        found_value = k.get_value_for_field(xml, "row", "Reputation")
        self.assertEqual(found_value, "5555")
        self.assertIsNone(k.get_value_for_field(xml, "row", "reputation"))
        self.assertIsNone(k.get_value_for_field(xml, "id", "Reputation"))
        self.assertIsNone(k.get_value_for_field("not xml", "a", "b"))

    def test_get_upvotes_and_downvotes_from_user_row(self):
        xml = '<row UpVotes="1" DownVotes="2" />'
        votes = k.get_upvotes_and_downvotes_from_user_row(xml)
        self.assertEqual(votes, (1, 2))
        self.assertEqual(k.get_upvotes_and_downvotes_from_user_row(""), (None, None))