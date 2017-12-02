import xmltodict


def parse_xml_line(line):
    line = cleanup_xml(line.strip())
    data = xmltodict.parse(line)
    return data


def cleanup_xml(line):
    replacements = {
        '&^Cquot': '&quot',
    }
    return reduce(lambda x, y: x.replace(y, replacements[y]), replacements, line)
