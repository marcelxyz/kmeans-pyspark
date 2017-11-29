import os
import xmltodict
import xml

file_path = "%s/../data/users.xml" % os.path.dirname(os.path.realpath(__file__))

with open(file_path) as f:
    for line in f:
        try:
            data = xmltodict.parse(line.strip())
            print(data["row"]["@DisplayName"])
            break
        except xml.parsers.expat.ExpatError as e:
            print(e)