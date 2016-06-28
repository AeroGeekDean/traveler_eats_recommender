#!/usr/bin/env python
# Adopted from http://blog.endpoint.com/2016/03/loading-json-files-into-postgresql-95.html
 
import os
import sys
import json
 
try:
    import psycopg2 as pg
    import psycopg2.extras
except:
    print "Install psycopg2"
    exit(123)
 
try:
    import progressbar
except:
    print "Install progressbar2"
    exit(123)
  

data_dir = 'test_data'
# data_dir = '/Users/deanliu/git/GalvanizeDSI/capstone_project/data/Yelp/yelp_dataset_challenge_academic_dataset'

# PG_CONN_STRING = "dbname='test_yelp' user='deanliu' host='localhost' port='5432'"
# dbconn = pg.connect(PG_CONN_STRING)

# db = 'yelp_data' # the ENTIRE Yelp academic dataset
db = 'yelp_test' # the small testing ver of Yelp academic dataset

dbconn = pg.connect(dbname=db, user='deanliu', host='localhost', port='5432')
cursor = dbconn.cursor()
 
file_counter = 0

# --- created SQL table ---
cursor.execute('''CREATE TABLE checkins (id INTEGER PRIMARY KEY, data JSONB NOT NULL);''')
cursor.execute('''CREATE TABLE businesses (id INTEGER PRIMARY KEY, data JSONB NOT NULL);''')
cursor.execute('''CREATE TABLE reviews (id INTEGER PRIMARY KEY, data JSONB NOT NULL);''')
cursor.execute('''CREATE TABLE tips (id INTEGER PRIMARY KEY, data JSONB NOT NULL);''')
cursor.execute('''CREATE TABLE users (id INTEGER PRIMARY KEY, data JSONB NOT NULL);''')


files = [f for f in os.listdir(data_dir) if os.path.isfile(os.path.join(data_dir, f))]
json_files = [f for f in files if f.endswith('.json')]

print 'Found {} files to process'.format(len(json_files))

for f in json_files:
    fname = os.path.join(data_dir, f)

    print 'Processing file: {}'.format(fname)
    bar_max = os.path.getsize(fname)
    bar = progressbar.ProgressBar(max_value = bar_max, 
            widgets=[
                ' [', progressbar.Timer(), '] ',
                progressbar.Bar(),
                ' (', progressbar.ETA(), ')',
            ])
    bar_progress = 0
    file_counter += 1

    with open(fname) as js_file:

        for line_count, js_line in enumerate(js_file):
            # print line_count, js_line
            bar_progress+=len(js_line)
            bar.update(bar_progress)
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
    bar.finish()
    dbconn.commit() # <--- this line saved my butt!

dbconn.commit()
dbconn.close()


# # Define a main() function that prints a little greeting.
# def main():
#   # Get the name from the command line, using 'World' as a fallback.
#   if len(sys.argv) >= 2:
#     name = sys.argv[1]
#   else:
#     name = 'World'
#   print 'Howdy', name
#   print 'yay!'

# # This is the standard boilerplate that calls the main() function.
# if __name__ == '__main__':
#   main()

