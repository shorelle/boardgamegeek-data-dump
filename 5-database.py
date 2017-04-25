import os
import datetime
import sys
import xmltodict
import json
import lxml.etree as le
from pymongo import MongoClient
from pymongo.errors import BulkWriteError

INPUT_PATH = 'BoardGameGeek.xml/%s/boardgame_batches/'

if len(sys.argv) > 1:
    DATE_DIR = sys.argv[1]
else:
    DATE_DIR = datetime.datetime.strftime(datetime.datetime.now(), '%Y%m')

input_dir = INPUT_PATH % DATE_DIR

# Set up MongoDB
client = MongoClient()
db = client.boardgamegeek

# Drop existing collection and recreate
db.boardgames.drop()
boardgames = db.boardgames

for filename in sorted(os.listdir(input_dir)):
    # Flatten XML by removing objectid and misc attributes (feel free to add back in)
    with open(os.path.join(input_dir, filename)) as file:
        doc = le.parse(file)
        for elem in doc.xpath('//*[attribute::objectid]'):
            elif elem.tag != 'boardgame':
                elem.attrib.pop('objectid')
        for elem in doc.xpath('//*[attribute::sortindex]'):
            elem.attrib.pop('sortindex')
        for elem in doc.xpath('//*[attribute::level]'):
            elem.attrib.pop('level')
    # Covert XML to JSON
    o_dict = xmltodict.parse(le.tostring(doc))
    json_str = json.dumps(o_dict, ensure_ascii=False)
    json_data = json.loads(json_str)
    # Insert in database
    try:
        result = boardgames.insert_many(json_data['boardgames']['boardgame'])
        print "Inserted %s" % filename
    except BulkWriteError as err:
        print err.details

client.close()