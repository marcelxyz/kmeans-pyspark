import xmltodict
from xml.parsers.expat import ExpatError


def parse_xml_line(line):
    try:
        line = cleanup_xml(line.strip())
        data = xmltodict.parse(line)
        return data
    except ExpatError as e:
        print(e, line.strip())


def cleanup_xml(line):
    replacements = {
        '&^Cquot': '&quot',
    }
    return reduce(lambda x, y: x.replace(y, replacements[y]), replacements, line)
