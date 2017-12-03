import xml.etree.ElementTree as ET


def extract_attributes(xml, attribute_names, cast=str):
    """
    Extracts a set of attribute values from an XML line.

    :param xml: XML line, e.g. <user id="1", rep="2" />
    :param attribute_names: List of attribute names to extract, e.g. ['id', 'rep']
    :param cast: callback to apply to each value
    :return: tuple of attribute values
    """
    data = parse_xml_line(xml)
    values = []

    if data is None:
        return tuple(values)

    for attribute_name in attribute_names:
        if attribute_name in data.attrib:
            value = data.attrib[attribute_name].strip()
            values.append(cast(value))
        else:
            values.append(None)

    return tuple(values)


def parse_xml_line(line):
    try:
        line = cleanup_xml(line.encode('utf-8').strip())
        return ET.fromstring(line)
    except ET.ParseError as e:
        # print(e, line.strip())
        return None


def cleanup_xml(line):
    replacements = {
        '&^Cquot': '&quot',
    }
    return reduce(lambda x, y: x.replace(y, replacements[y]), replacements, line)
