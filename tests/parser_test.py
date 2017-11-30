import unittest
import parser


class ParserTest(unittest.TestCase):
    def test_parse_xml_line__correctly_parses_xml(self):
        xml = '<user id="19" date="2009-01-12T19:07:38.647" name="Tassos" />'
        data = parser.parse_xml_line(xml)

        self.assertIn('user', data)
        self.assertEqual(data['user']['@id'], '19')
        self.assertEqual(data['user']['@date'], '2009-01-12T19:07:38.647')
        self.assertEqual(data['user']['@name'], 'Tassos')

    def test_parse_xml_line__returns_none_when_xml_malformed(self):
        xml = '<user>'
        self.assertIsNone(parser.parse_xml_line(xml))

    def test_cleanup_xml(self):
        before = '<p>lol&^Cquot</p>'
        after = '<p>lol&quot</p>'
        self.assertEqual(parser.cleanup_xml(before), after)
