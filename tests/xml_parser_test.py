import unittest
import xml_parser


class ParserTest(unittest.TestCase):
    def test_extract_attributes(self):
        xml = '<user id="19" date="  2009-01-12T19:07:38.647  " name="Tassos  " />'
        values = xml_parser.extract_attributes(xml, ["date", "name"])
        self.assertEqual(values, ("2009-01-12T19:07:38.647", "Tassos"))

    def test_extract_attributes_casts_values(self):
        xml = '<user id="19" rep="90050" year="2017" />'
        values = xml_parser.extract_attributes(xml, ["id", "rep", "year"], int)
        self.assertEqual(values, (19, 90050, 2017))
