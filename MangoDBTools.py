import pymongo
import json
from pymongo import MongoClient


def main():
    client = MongoClient('10.1.10.61', 27017)
    db = client['cloudcar']
    collection = db['search_history']
    cursor = collection.find({})
    with open('./searchResult.json', 'w') as outfile:
        outfile.write("[\n")
        for document in cursor:
            data = {}
            # if 'searchQuery' in document and 'searchResult' in document :
            if 'searchQuery' in document and 'searchResult' in document:
                if document['searchQuery'] is not None and bool(document['searchQuery'].strip()) :
                    print('search keyword is: {}'.format(document['searchQuery']))
                    data['searchQuery'] = document['searchQuery']
                    # data['result'] = document['searchResult']
                    json.dump(data, outfile, indent=4)
                    outfile.write(",\n")
        outfile.write("]")
if __name__ == "__main__":
    main()