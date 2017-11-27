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
    attrib_rows = soup.row[attrib_name]
    for attrib_row in attrib_rows:
        print(attrib_row)    

# Treat as 'main' method.
if __name__ == '__main__':
    f_path = 'user_sample.xml'
    data = getfile(f_path)
    printallrowattrib(data, 'reputation')
    data.close()
