def extract_attribute_value(xml, attribute_name):
    beg = '%s="' % attribute_name
    end = '" '

    start_index = xml.find(beg)
    if start_index == -1:
        return None

    substring = xml[start_index + len(beg):len(xml)]

    end_index = substring.find(end)
    if end_index == -1:
        return None

    return substring[0:end_index]


def extract_attributes(xml, attribute_names, cast=str):
    """
    Extracts a set of attribute values from an XML line.

    :param xml: XML line, e.g. <user id="1", rep="2" />
    :param attribute_names: List of attribute names to extract, e.g. ['id', 'rep']
    :param cast: callback to apply to each value
    :return: tuple of attribute values
    """
    values = []

    for attribute_name in attribute_names:
        value = extract_attribute_value(xml, attribute_name)
        if value:
            values.append(cast(value.encode('utf-8').strip()))

    return tuple(values)
