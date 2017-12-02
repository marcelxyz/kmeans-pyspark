import parser
from xml.parsers.expat import ExpatError


def get_value_for_field(xml, element_name, attribute_name):
    attribute_name = "@" + attribute_name

    try:
        data = parser.parse_xml_line(xml)
        if element_name in data and attribute_name in data[element_name]:
            return data[element_name][attribute_name].strip()
    except ExpatError as e:
        print(e, xml.strip())

    return None


def get_upvotes_and_downvotes_from_user_row(xml):
    upvotes = get_value_for_field(xml, "row", "UpVotes")
    downvotes = get_value_for_field(xml, "row", "DownVotes")
    try:
        return int(upvotes), int(downvotes)
    except:
        return None, None  # todo refactor to just none?
