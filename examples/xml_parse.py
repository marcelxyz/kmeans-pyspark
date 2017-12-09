import os
import xml_parser

file_path = "%s/../data/users.xml" % os.path.dirname(os.path.realpath(__file__))

with open(file_path) as f:
    for line in f:
        data = xml_parser.parse_xml_line(line)
        # print(data)