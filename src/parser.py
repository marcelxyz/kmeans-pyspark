import xmltodict
from xml.parsers.expat import ExpatError


def extract_attributes(xml, element_name, attribute_names, cast=str):
    """
    Extracts a set of attribute values from an XML line.

    :param xml: XML line, e.g. <user id="1", rep="2" />
    :param element_name: Name of the main element in the line, e.g. 'user'
    :param attribute_names: List of attribute names to extract, e.g. ['id', 'rep']
    :param cast: callback to apply to each value
    :return: tuple of attribute values
    """
    data = parse_xml_line(xml)
    values = []

    if data is None or element_name not in data:
        return ()

    for attribute_name in attribute_names:
        attribute_name = "@" + attribute_name

        if attribute_name in data[element_name]:
            value = data[element_name][attribute_name].strip()
            values.append(cast(value))
        else:
            values.append(None)

    return tuple(values)


def parse_xml_line(line):
    try:
        line = cleanup_xml(line.strip())
        return xmltodict.parse(line)
    except ExpatError as e:
        # print(e, line.strip())
        return None


def cleanup_xml(line):
    replacements = {
        '&^Cquot': '&quot',
    }
    return reduce(lambda x, y: x.replace(y, replacements[y]), replacements, line)
