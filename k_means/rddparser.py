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
    
    users_path = "/Users/sameenislam/Documents/Big_Data/cw2/sample_data/user_sample.xml"
    conf = SparkConf().setAppName("rddparser")
    sc = SparkContext(conf=conf)
    raw_data = sc.textFile(users_path)
    print('Total count: ' + str(raw_data.count()))
    xml_data = raw_data.map(transformXmlToMap)

    t0 = time()
    user_rows = xml_data.take(5)
    tt = time() - t0
    print("Parse completed in {} seconds".format(round(tt,3)))
    for x in range(0, 4):
        pprint(user_rows[x])
    
    