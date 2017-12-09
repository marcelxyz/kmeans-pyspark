import unittest
import xml_parser


class ParserTest(unittest.TestCase):
    def test_parse_xml_line__correctly_parses_xml(self):
        xml = '<user id="19" date="2009-01-12T19:07:38.647" name="Tassos" />'
        data = xml_parser.parse_xml_line(xml)

        self.assertEqual(data.tag, 'user')
        self.assertEqual(data.attrib['id'], '19')
        self.assertEqual(data.attrib['date'], '2009-01-12T19:07:38.647')
        self.assertEqual(data.attrib['name'], 'Tassos')

    def test_parse_xml_line__returns_none_when_xml_malformed(self):
        xml = '<user>'
        self.assertIsNone(xml_parser.parse_xml_line(xml))

    def test_extract_attributes(self):
        xml = '<user id="19" date="  2009-01-12T19:07:38.647  " name="Tassos  " />'
        values = xml_parser.extract_attributes(xml, ["date", "name"])
        self.assertEqual(values, ("2009-01-12T19:07:38.647", "Tassos"))

    def test_extract_attributes_casts_values(self):
        xml = '<user id="19" rep="90050" year="2017" />'
        values = xml_parser.extract_attributes(xml, ["id", "rep", "year"], int)
        self.assertEqual(values, (19, 90050, 2017))

    def test_cleanup_xml(self):
        before = '<p>lol&^Cquot</p>'
        after = '<p>lol&quot</p>'
        self.assertEqual(xml_parser.cleanup_xml(before), after)
