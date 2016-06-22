#!/usr/bin/env python
# Found this script from http://blog.endpoint.com/2016/03/loading-json-files-into-postgresql-95.html
#
 
import os
import sys
import logging
 
try:
    import psycopg2 as pg
    import psycopg2.extras
except:
    print "Install psycopg2"
    exit(123)
 
# try:
#     import progressbar
# except:
#     print "Install progressbar2"
#     exit(123)
 
import json

#----------
# class ProgressInfo:
 
#     def __init__(self, dir):
#         files_no = 0
#         for root, dirs, files in os.walk(dir):
#             for file in files:
#                 if file.endswith(".json"):
#                     files_no += 1
#         self.files_no = files_no
#         print "Found {} files to process".format(self.files_no)
#         self.bar = progressbar.ProgressBar(maxval=self.files_no,
#                                            widgets=[' [', progressbar.Timer(), '] [', progressbar.ETA(), '] ', progressbar.Bar(),])
 
#     def update(self, file_counter):
#         self.bar.update(file_counter)

#---------- 
logger = logging.getLogger()
 
data_dir = 'test_data'
# data_dir = '/Users/deanliu/git/GalvanizeDSI/capstone_project/data/Yelp/yelp_dataset_challenge_academic_dataset'

logger.info("Loading data from '{}'".format(data_dir))

# PG_CONN_STRING = "dbname='test_yelp' user='deanliu' host='localhost' port='5432'"
# dbconn = pg.connect(PG_CONN_STRING)

# db = 'yelp_data' # the ENTIRE Yelp academic dataset
db = 'yelp_test' # the small testing ver of Yelp academic dataset

dbconn = pg.connect(dbname=db, user='deanliu', host='localhost', port='5432')
cursor = dbconn.cursor()
 
file_counter = 0

# pi = ProgressInfo(os.path.expanduser(data_dir))

# --- created SQL table ---
cursor.execute('''CREATE TABLE checkins (id INTEGER PRIMARY KEY, data JSONB NOT NULL);''')
cursor.execute('''CREATE TABLE businesses (id INTEGER PRIMARY KEY, data JSONB NOT NULL);''')
cursor.execute('''CREATE TABLE reviews (id INTEGER PRIMARY KEY, data JSONB NOT NULL);''')
cursor.execute('''CREATE TABLE tips (id INTEGER PRIMARY KEY, data JSONB NOT NULL);''')
cursor.execute('''CREATE TABLE users (id INTEGER PRIMARY KEY, data JSONB NOT NULL);''')


files = [f for f in os.listdir(data_dir) if os.path.isfile(os.path.join(data_dir, f))]
files = [f for f in files if f.endswith('.json')] # look only at '.json' files

for f in files:
    fname = os.path.join(data_dir, f)

    print 'Processing file: {}'.format(fname)
    # pi.update(file_counter)
    file_counter += 1

    with open(fname) as js_file:

        for line_count, js_line in enumerate(js_file):
            # print line_count, js_line
            js_type = json.loads(js_line)['type']

            if js_type == 'checkin':
                cursor.execute("""INSERT INTO checkins(id, data) VALUES (%s, %s);""", (line_count, js_line))
            elif js_type == 'business':
                cursor.execute("""INSERT INTO businesses(id, data) VALUES (%s, %s);""", (line_count, js_line))
            elif js_type == 'review':
                cursor.execute("""INSERT INTO reviews(id, data) VALUES (%s, %s);""", (line_count, js_line))
            elif js_type == 'tip':
                cursor.execute("""INSERT INTO tips(id, data) VALUES (%s, %s);""", (line_count, js_line))
            elif js_type == 'user':
                cursor.execute("""INSERT INTO users(id, data) VALUES (%s, %s);""", (line_count, js_line))
            else: # no js_typ match, FAULT!
                pass
    dbconn.commit() # <--- this line saved my butt!

# pi.update(file_counter)\
 
dbconn.commit()
dbconn.close()
 
logger.info("Loaded {} files".format(file_counter))
