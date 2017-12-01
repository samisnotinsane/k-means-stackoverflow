from pyspark.sql import SparkSession

appName = "rddparser"
usersFile = "/Users/sameenislam/Documents/Big_Data/cw2/sample_data/user_sample.xml"
postsFile = "/Users/sameenislam/Documents/Big_Data/cw2/sample_data/posts_sample.xml"

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

    spark = SparkSession.builder.appName(appName).getOrCreate()
    usersDataFrame = spark.read.text(usersFile).cache()
    print(usersDataFrame)
    print(usersDataFrame.show())
    rows = usersDataFrame.filter(usersDataFrame.value.contains('row')).count()
    print("Lines with row: %i," % (rows))
    spark.stop()




