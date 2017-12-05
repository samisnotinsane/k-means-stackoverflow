from pyspark import SparkContext, SparkConf
from time import time
from pprint import pprint

if __name__ == '__main__':
    def transformXmlToMap(line):
        line_nowhitespace = line.strip() # Remove whitespaces.
        line_noxml = line_nowhitespace[5:-3] # Discard XML tag.
        raw_elements = line_noxml.split("\"") # Use " as delimiter.
        i = 0
        d = {}
        while i < (len(raw_elements) - 1):
            key = raw_elements[i].strip()[:-1] # Gets rid of whitespace &'=' sign.
            value = raw_elements[i+1]
            # Uncomment print lines for debugging.
            # print('key:' + key)
            # print('value:' + value)
            d[key] = value
            i += 2
        return d
    
    # Commonly used paths:
    # /data/stackOverflow2017/Users.xml
    # so2017/users_sample.xml
    users_path = "/Users/sameenislam/Documents/Big_Data/cw2/sample_data/user_sample.xml"
    conf = SparkConf().setAppName("rddparser")
    sc = SparkContext(conf=conf)
    raw_data = sc.textFile(users_path)
    print('Total count: ' + str(raw_data.count()))
    xml_data = raw_data.map(transformXmlToMap).cache()

    total_count = raw_data.count()
    print('Total count: ' + str(total_count))
    grouped_data = xml_data.groupByKey()
    