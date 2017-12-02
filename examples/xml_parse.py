import os
import parser

file_path = "%s/../data/users.xml" % os.path.dirname(os.path.realpath(__file__))

with open(file_path) as f:
    for i, line in enumerate(f):
        if i == 5:
            data = parser.parse_xml_line(line)
            print(data["row"])