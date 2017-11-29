from bs4 import BeautifulSoup

"""
" Opens a file and returns an object reference to it.
"""
def getfile(file_path):
    data = open(file_path, "r")
    return data

"""
" Given a file, prints all attributes for specified row.
"""
def printallrowattrib(data, attrib_name):
    soup = BeautifulSoup(data, 'html.parser')
    all_rows = soup.find_all('row')
    for attrib_row in all_rows:
        print(attrib_row.get(attrib_name))    

# Treat as 'main' method.
if __name__ == '__main__':
    f_path = '/Users/sameenislam/Documents/Big_Data/cw2/sample_data/posts_sample.xml'
    data = getfile(f_path)
    printallrowattrib(data, 'owneruserid')
    data.close()
