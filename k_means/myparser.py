"""
' Algorithm based on MRDPUtils in Mapreduce Design Patterns (O'Reilly): D. Miner & A. Shook.
' Parses XML row data from Stack Overflow.
"""
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

# Treat as 'main' method.
if __name__ == '__main__':
    file_path = '/Users/sameenislam/Documents/Big_Data/cw2/sample_data/user_sample.xml'
    with open(file_path, "r") as f:
        for line in f:
            row_dict = transformXmlToMap(line)
            aid = row_dict['AccountId']
            print(aid)
    